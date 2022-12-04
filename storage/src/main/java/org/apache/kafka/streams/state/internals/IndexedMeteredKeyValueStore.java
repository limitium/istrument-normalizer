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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;


public class IndexedMeteredKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> implements IndexedKeyValueStore<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(IndexedMeteredKeyValueStore.class);
    private final String metricsScope;
    private Sensor rebuildIndexSensor;
    private Sensor lookupIndexSensor;
    private Sensor updateIndexSensor;
    private Sensor removeIndexSensor;

    private final Map<String, Map<String, Bytes>> uniqIndexesData = new HashMap<>();
    private final Map<String, Function<V, String>> uniqIndexes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean indexesBuilded = false;

    IndexedMeteredKeyValueStore(final Map<String, Function<V, String>> uniqIndexes,
                                final KeyValueStore<Bytes, byte[]> inner,
                                final String metricsScope,
                                final Time time,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        super(inner, metricsScope, time, keySerde, valueSerde);

        this.metricsScope = metricsScope;
        this.uniqIndexes = ImmutableMap.copyOf(uniqIndexes);
        this.uniqIndexes.forEach((key, value) -> uniqIndexesData.put(key, new HashMap<>()));
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        super.init(context, root);
        TaskId taskId = context.taskId();
        StreamsMetricsImpl streamsMetrics = (StreamsMetricsImpl) context.metrics();

        rebuildIndexSensor = StateStoreMetrics2.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        lookupIndexSensor = StateStoreMetrics2.lookupIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        updateIndexSensor = StateStoreMetrics2.updateIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeIndexSensor = StateStoreMetrics2.removeIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
    }

    @Override
    public void rebuildIndexes() {
        maybeMeasureLatency(this::rebuildIndexesInternal, time, rebuildIndexSensor);
    }


    @Override
    public V getUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilded) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.rebuildIndexes() from Processor#init() method");
        }
        try {
            K key = maybeMeasureLatency(() -> lookupKey(indexName, indexKey), time, lookupIndexSensor);
            if (key == null) {
                return null;
            }

            return get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void rebuildIndexesInternal() {
        lock.writeLock().lock();

        indexesBuilded = true;
        try {
            uniqIndexesData.values().forEach(Map::clear);
            try (KeyValueIterator<Bytes, byte[]> kvIterator = wrapped().all()) {
                while (kvIterator.hasNext()) {
                    KeyValue<Bytes, byte[]> kv = kvIterator.next();
                    updateUniqIndexes(kv);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private K lookupKey(String indexName, String indexKey) {
        Map<String, Bytes> index = uniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        Bytes keyBytes = index.get(indexKey);
        if (keyBytes == null) {
            return null;
        }

        //Extra ser here, shouldn't be too expensive otherwise override the whole method {@link MeteredKeyValueStore#get(K key)}
        return deserKey(keyBytes);
    }

    @Override
    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            super.put(key, value);
            maybeMeasureLatency(() -> updateUniqIndexes(key, value), time, updateIndexSensor);
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
                maybeMeasureLatency(() -> removeUniqIndex(deleted), time, removeIndexSensor);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void removeUniqIndex(V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            indexData.remove(generateIndexKey(indexName, value));
        });
    }

    private void updateUniqIndexes(KeyValue<Bytes, byte[]> kv) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            V value = valueSerde.deserializer().deserialize(null, kv.value);
            String indexKey = generateIndexKey(indexName, value);
            Bytes prevStoredKey = indexData.put(indexKey, kv.key);
            if (prevStoredKey != null && !kv.key.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + deserKey(kv.key) + ", old key:" + deserKey(prevStoredKey) + ", value:" + deserValue(kv.value));
            }
        });
    }

    private void updateUniqIndexes(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            Bytes keyBytes = keyBytes(key);
            String indexKey = generateIndexKey(indexName, value);

            Bytes prevStoredKey = indexData.put(indexKey, keyBytes);
            if (prevStoredKey != null && !keyBytes.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + key + ", old key:" + deserKey(prevStoredKey) + ", value:" + value);
            }
        });
    }

    private String generateIndexKey(String indexName, V value) {
        Function<V, String> keyGenerator = uniqIndexes.get(indexName);
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
