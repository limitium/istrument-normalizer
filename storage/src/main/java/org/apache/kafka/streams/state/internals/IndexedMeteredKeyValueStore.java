package org.apache.kafka.streams.state.internals;


import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.UniqKeyViolationException;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;


public class IndexedMeteredKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> implements IndexedKeyValueStore<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(IndexedMeteredKeyValueStore.class);
    private final String metricsScope;
    private Sensor rebuildUniqIndexSensor;
    private Sensor lookupUniqIndexSensor;
    private Sensor updateUniqIndexSensor;
    private Sensor removeUniqIndexSensor;
    private Sensor rebuildNonUniqIndexSensor;
    private Sensor lookupNonUniqIndexSensor;
    private Sensor updateNonUniqIndexSensor;
    private Sensor removeNonUniqIndexSensor;

    private final Map<String, Map<String, Bytes>> uniqIndexesData = new HashMap<>();
    private final Map<String, Map<String, Set<Bytes>>> nonUniqIndexesData = new HashMap<>();
    private final Map<String, Function<V, String>> uniqIndexes;
    private final Map<String, Function<V, String>> nonUniqIndexes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean indexesBuilt = false;

    IndexedMeteredKeyValueStore(final Map<String, Function<V, String>> uniqIndexes,
                                final Map<String, Function<V, String>> nonUniqIndexes,
                                final KeyValueStore<Bytes, byte[]> inner,
                                final String metricsScope,
                                final Time time,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        super(inner, metricsScope, time, keySerde, valueSerde);
        logger.debug("Store `{}` created with {} uniq, {} non uniq indexes", name(), uniqIndexes.size(), nonUniqIndexes.size());

        this.metricsScope = metricsScope;
        this.uniqIndexes = ImmutableMap.copyOf(uniqIndexes);
        this.nonUniqIndexes = ImmutableMap.copyOf(nonUniqIndexes);

        this.uniqIndexes.forEach((name, generator) -> uniqIndexesData.put(name, new HashMap<>()));
        this.nonUniqIndexes.forEach((name, generator) -> nonUniqIndexesData.put(name, new HashMap<>()));
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        super.init(context, root);
        TaskId taskId = context.taskId();
        StreamsMetricsImpl streamsMetrics = (StreamsMetricsImpl) context.metrics();

        rebuildUniqIndexSensor = StateStoreMetrics2.restoreUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        lookupUniqIndexSensor = StateStoreMetrics2.lookupUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        updateUniqIndexSensor = StateStoreMetrics2.updateUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeUniqIndexSensor = StateStoreMetrics2.removeUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        rebuildNonUniqIndexSensor = StateStoreMetrics2.restoreNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        lookupNonUniqIndexSensor = StateStoreMetrics2.lookupNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        updateNonUniqIndexSensor = StateStoreMetrics2.updateNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeNonUniqIndexSensor = StateStoreMetrics2.removeNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
    }

    @Override
    public void rebuildIndexes() {
        lock.writeLock().lock();

        try {
            uniqIndexesData.values().forEach(Map::clear);
            nonUniqIndexesData.values().forEach(Map::clear);

            try (KeyValueIterator<Bytes, byte[]> kvIterator = wrapped().all()) {
                while (kvIterator.hasNext()) {
                    KeyValue<Bytes, byte[]> kv = kvIterator.next();

                    maybeMeasureLatency(() -> updateUniqIndexes(kv), time, rebuildUniqIndexSensor);
                    maybeMeasureLatency(() -> updateNonUniqIndexes(kv), time, rebuildNonUniqIndexSensor);
                }
            }
            indexesBuilt = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V getUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.rebuildIndexes() from Processor#init() method");
        }
        try {
            K key = maybeMeasureLatency(() -> lookupUniqKey(indexName, indexKey), time, lookupUniqIndexSensor);
            if (key == null) {
                return null;
            }

            return get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<V> getNonUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.rebuildIndexes() from Processor#init() method");
        }
        try {
            Stream<K> keys = maybeMeasureLatency(() -> lookupNonUniqKeys(indexName, indexKey), time, lookupNonUniqIndexSensor);

            return keys.map(super::get);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            super.put(key, value);
            maybeMeasureLatency(() -> updateUniqIndexes(key, value), time, updateUniqIndexSensor);
            maybeMeasureLatency(() -> updateNonUniqIndexes(key, value), time, updateUniqIndexSensor);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public V delete(K key) {
        lock.writeLock().lock();
        try {
            V deleted = super.delete(key);
            if (deleted != null) {
                maybeMeasureLatency(() -> removeUniqIndex(key, deleted), time, removeUniqIndexSensor);
                maybeMeasureLatency(() -> removeNonUniqIndex(key, deleted), time, removeUniqIndexSensor);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }


    private K lookupUniqKey(String indexName, String indexKey) {
        Map<String, Bytes> index = uniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        Bytes keyBytes = index.get(indexKey);
        if (keyBytes == null) {
            return null;
        }

        //Extra ser here, shouldn't be too expensive otherwise override the whole method {@link MeteredKeyValueStore#get(K key)}
        return deserKey(keyBytes);
    }

    private Stream<K> lookupNonUniqKeys(String indexName, String indexKey) {
        Map<String, Set<Bytes>> index = nonUniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        Set<K> keys = Optional.ofNullable(index.get(indexKey))
                .orElse(Collections.emptySet())
                .stream()
                .map(this::deserKey)
                //Terminate stream to calculate lookup footprint
                .collect(Collectors.toSet());

        return keys.stream();
    }

    private void removeUniqIndex(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);

            logger.debug("Remove from uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            indexData.remove(indexKey);
        });
    }

    private void removeNonUniqIndex(K key, V value) {
        nonUniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(nonUniqIndexes, indexName, value);

            logger.debug("Remove from non uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            Set<Bytes> keys = indexData.get(indexKey);
            if (keys != null) {
                keys.remove(keyBytes(key));
                if (keys.isEmpty()) {
                    indexData.remove(indexKey);
                }
            }
        });
    }


    private void updateUniqIndexes(KeyValue<Bytes, byte[]> kv) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            V value = valueSerde.deserializer().deserialize(null, kv.value);
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);
            Bytes prevStoredKey = indexData.put(indexKey, kv.key);

            logger.debug("Update uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, kv.key, value);
            if (prevStoredKey != null && !kv.key.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + deserKey(kv.key) + ", old key:" + deserKey(prevStoredKey) + ", value:" + deserValue(kv.value));
            }
        });
    }

    private void updateNonUniqIndexes(KeyValue<Bytes, byte[]> kv) {
        nonUniqIndexesData.forEach((indexName, indexData) -> {
            V value = valueSerde.deserializer().deserialize(null, kv.value);
            String indexKey = generateIndexKey(nonUniqIndexes, indexName, value);

            logger.debug("Update non uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, kv.key, value);
            insertNonUniqKey(indexData, indexKey, kv.key);
        });
    }

    private boolean insertNonUniqKey(Map<String, Set<Bytes>> indexData, String indexKey, Bytes key) {
        if (!indexData.containsKey(indexKey)) {
            indexData.put(indexKey, new HashSet<>());
        }
        return indexData.get(indexKey).add(key);
    }

    private void updateUniqIndexes(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            Bytes keyBytes = keyBytes(key);
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);

            logger.debug("Update uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);

            Bytes prevStoredKey = indexData.put(indexKey, keyBytes);
            if (prevStoredKey != null && !keyBytes.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + key + ", old key:" + deserKey(prevStoredKey) + ", value:" + value);
            }
        });
    }

    private void updateNonUniqIndexes(K key, V value) {
        nonUniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(nonUniqIndexes, indexName, value);

            logger.debug("Update non uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);
            insertNonUniqKey(indexData, indexKey, keyBytes(key));
        });
    }

    private String generateIndexKey(Map<String, Function<V, String>> indexes, String indexName, V value) {
        Function<V, String> keyGenerator = indexes.get(indexName);
        String indexKey = keyGenerator.apply(value);
        Objects.requireNonNull(indexKey, "Null keys are not supported. Problem with an index:" + indexName);
        return indexKey;
    }


    private K deserKey(Bytes key) {
        return keySerde.deserializer().deserialize(null, key.get());
    }

    private V deserValue(byte[] value) {
        return valueSerde.deserializer().deserialize(null, value);
    }
}
