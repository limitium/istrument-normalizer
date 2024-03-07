package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.UniqKeyViolationException;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics2;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

@SuppressWarnings("rawtypes")
public class IndexedMeteredKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> implements IndexedKeyValueStore<K, V>, ProcessorPostInitListener {
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
    private final Map<String, UniqIndex<K, V>> uniqIndexesData = new HashMap<>();
    private final Map<String, NonUniqIndex<K, V>> nonUniqIndexesData = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean indexesBuilt = false;
    private ProcessorContext processorContext;

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

        uniqIndexes.forEach((name, keyGenerator)->uniqIndexesData.put(name, new UniqIndex<>(name,keyGenerator)));
        nonUniqIndexes.forEach((name, keyGenerator)->nonUniqIndexesData.put(name, new NonUniqIndex<>(name,keyGenerator)));
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
    public V getUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.onPostInit(context) from Processor#init() method");
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
    public K getUniqKey(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if(!indexesBuilt){
            throw new RuntimeException("Indexes of "+name()+" were not built, call IndexedKeyValueStore.onPostInit() from Processor#init() method");
        }
        try{
            return maybeMeasureLatency(()->lookupUniqKey(indexName,indexKey),time,lookupUniqIndexSensor);
        }finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<V> getNonUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes of" + name() + " were not built, call IndexedKeyValueStore.onPostInit() from Processor#init() method");
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
            maybeMeasureLatency(() -> updateNonUniqIndexes(key, value), time, updateNonUniqIndexSensor);
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
                maybeMeasureLatency(() -> removeNonUniqIndex(key, deleted), time, removeNonUniqIndexSensor);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onPostInit(ProcessorContext processorContext) {
        this.processorContext = processorContext;

        uniqIndexesData.forEach((indexName, indexData) -> {
            indexData.data = this.processorContext.getStateStore(name() + "_" + indexName);
        });
        rebuildIndexes();
    }

    private void rebuildIndexes() {
        long rebuildStart = System.currentTimeMillis();
        lock.writeLock().lock();

        logger.info("Rebuilding indexes");
        try {
            boolean uniqIndexesRebuildRequired = uniqIndexesData
                    .values()
                    .stream()
                    .map(kvIndexData -> kvIndexData.data.approximateNumEntries())
                    .allMatch(count -> count == 0); //nothing was restored from changelogs

            boolean nonUniqIndexesRebuildRequired = true;

            if (uniqIndexesRebuildRequired) {
                uniqIndexesData.values().forEach(indexData -> {
                    //should be empty on each call
                    try (KeyValueIterator<String, K> all = indexData.data.all()) {
                        all.forEachRemaining(stringKKeyValue -> {
                            indexData.data.delete(stringKKeyValue.key);
                        });
                    }
                });
            }

            if (nonUniqIndexesRebuildRequired) {
                nonUniqIndexesData.values().forEach(idx->idx.data.clear());
            }

            if (uniqIndexesRebuildRequired || nonUniqIndexesRebuildRequired) {
                try (KeyValueIterator<Bytes, byte[]> kvIterator = wrapped().all()) {
                    while (kvIterator.hasNext()) {
                        KeyValue<Bytes, byte[]> kv = kvIterator.next();

                        K key = deserKey(kv.key);
                        V value = deserValue(kv.value);

                        if (uniqIndexesRebuildRequired) {
                            maybeMeasureLatency(() -> updateUniqIndexes(key, value), time, rebuildUniqIndexSensor);
                        }
                        if (nonUniqIndexesRebuildRequired) {
                            maybeMeasureLatency(() -> updateNonUniqIndexes(key, value), time, rebuildNonUniqIndexSensor);
                        }
                    }
                }
            }
            indexesBuilt = true;
        } finally {
            lock.writeLock().unlock();
            logger.info("Rebuild indexes {} finished of {} for {}ms", processorContext.taskId(), name(), System.currentTimeMillis() - rebuildStart);
        }
    }

    private K lookupUniqKey(String indexName, String indexKey) {
        KeyValueStore<String, K> index = uniqIndexesData.get(indexName).data;
        Objects.requireNonNull(index, "Index not found:" + indexName);

        return index.get(indexKey);
    }

    private Stream<K> lookupNonUniqKeys(String indexName, String indexKey) {
        NonUniqIndex<K, V> index = nonUniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        Set<K> keys = new HashSet<>(Optional.ofNullable(index.data.get(indexKey))
                .orElse(Collections.emptySet()));

        return keys.stream();
    }

    private void removeUniqIndex(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(indexData.keyGenerator, indexName, value);

            logger.debug("Remove from uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            indexData.data.delete(indexKey);
        });
    }

    private void removeNonUniqIndex(K key, V value) {
        nonUniqIndexesData.forEach((indexName, idx) -> {
            String indexKey = generateIndexKey(idx.keyGenerator, indexName, value);

            logger.debug("Remove from non uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            Set<K> keys = idx.data.get(indexKey);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    idx.data.remove(indexKey);
                }
            }
        });
    }

    private boolean insertNonUniqKey(Map<String, Set<K>> indexData, String indexKey, Bytes key) {
        if (!indexData.containsKey(indexKey)) {
            indexData.put(indexKey, new HashSet<>());
        }
        return indexData.get(indexKey).add(keySerde.deserializer().deserialize(null, key.get()));
    }

    private void updateUniqIndexes(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(indexData.keyGenerator, indexName, value);

            logger.debug("Update uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);

            K prevStoredKey = indexData.data.get(indexKey);

            indexData.data.put(indexKey, key);
            if (prevStoredKey != null && !key.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + key + ", old key:" + prevStoredKey + ", value:" + value);
            }
        });
    }

    private void updateNonUniqIndexes(K key, V value) {
        nonUniqIndexesData.forEach((indexName, idx) -> {
            String indexKey = generateIndexKey(idx.keyGenerator, indexName, value);

            logger.debug("Update non uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);
            insertNonUniqKey(idx.data, indexKey, keyBytes(key));
        });
    }

    private String generateIndexKey(Map<String, Function<V, String>> indexes, String indexName, V value) {
        Function<V, String> keyGenerator = indexes.get(indexName);
        return generateIndexKey(keyGenerator, indexName, value);
    }

    @NotNull
    private String generateIndexKey(Function<V, String> keyGenerator, String indexName, V value) {
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

    static class UniqIndex<K, V> {
        final String name;
        final Function<V, String> keyGenerator;
        KeyValueStore<String, K> data;

        public UniqIndex(String name, Function<V, String> keyGenerator) {
            this.name = name;
            this.keyGenerator = keyGenerator;
        }
    }

    static class NonUniqIndex<K, V> {
        final String name;
        final Function<V, String> keyGenerator;
        final Map<String, Set<K>> data = new HashMap<>();

        public NonUniqIndex(String name, Function<V, String> keyGenerator) {
            this.name = name;
            this.keyGenerator = keyGenerator;
        }
    }
}
