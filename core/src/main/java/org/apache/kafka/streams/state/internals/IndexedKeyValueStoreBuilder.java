package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndexedKeyValueStoreBuilder<K, V> extends InjectableKeyValueStoreBuilder<K, V, KeyValueStore<K,V>> {
    protected final HashMap<String, IndexData<K,V>> uniqIndexes;
    protected final HashMap<String, Function<V, String>> nonUniqIndexes;
    public IndexedKeyValueStoreBuilder(KeyValueBytesStoreSupplier storeSupplier, Serde keySerde, Serde valueSerde, Time time) {
        super(storeSupplier, keySerde, valueSerde, time);
        this.uniqIndexes = new HashMap<>();
        this.nonUniqIndexes = new HashMap<>();
    }

    /**
     * Add a new uniq index based on generated {@code keyGenerator} key, with {@code indexName} name
     * @param indexName
     * @param keyGenerator converts value into the index key, the same key should be used for value extraction via {@link IndexedMeteredKeyValueStore#getUnique(String, String)}
     * @return
     */
    public IndexedKeyValueStoreBuilder<K, V> addUniqIndex(String indexName, Function<V, String> keyGenerator) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(keyGenerator, "keyGenerator cannot be null");

        if (uniqIndexes.containsKey(indexName)) {
            throw new RuntimeException("Index with the name `" + indexName + "` already exits");
        }

        uniqIndexes.put(indexName, new IndexData<>(keyGenerator,Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(name + "_" + indexName), Serdes.String(), keySerde)));
        return this;
    }

    /**
     * Add a new non uniq index based on generated {@code keyGenerator} key, with {@code indexName} name
     *
     * @param indexName
     * @param keyGenerator converts value into the index key, the same key should be used for value extraction via {@link IndexedMeteredKeyValueStore#getNonUnique(String, String)} (String, String)}
     * @return
     */
    public IndexedKeyValueStoreBuilder<K, V> addNonUniqIndex(String indexName, Function<V, String> keyGenerator) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(keyGenerator, "keyGenerator cannot be null");

        if (nonUniqIndexes.containsKey(indexName)) {
            throw new RuntimeException("Index with the name `" + indexName + "` already exits");
        }

        nonUniqIndexes.put(indexName, keyGenerator);
        return this;
    }

    @Override
    public IndexedMeteredKeyValueStore<K, V> build() {
        return new IndexedMeteredKeyValueStore<>(
                getIndexGenerators(),
                nonUniqIndexes,
                maybeWrapCaching(maybeWrapLogging(storeSupplier.get())),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
    }

    protected HashMap<String, Function<V, String>> getIndexGenerators() {
        return uniqIndexes.entrySet().stream().collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue().keyGenerator()), HashMap::putAll);
    }

    public Set<StoreBuilder<KeyValueStore<String, K>>> getIndexBuilders(){
        return uniqIndexes.values().stream().map(IndexData::idxStoreBuilder).collect(Collectors.toSet());
    }

    @Override
    public IndexedKeyValueStoreBuilder<K, V> withCachingEnabled() {
         super.withCachingEnabled();
         return this;
    }

    @Override
    public IndexedKeyValueStoreBuilder<K, V> withCachingDisabled() {
        super.withCachingDisabled();
        return this;
    }

    @Override
    public IndexedKeyValueStoreBuilder<K, V> withLoggingEnabled(Map<String, String> config) {
        super.withLoggingEnabled(config);
        return this;
    }

    @Override
    public IndexedKeyValueStoreBuilder<K, V> withLoggingDisabled() {
        super.withLoggingDisabled();
        return this;
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner, false);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingKeyValueBytesStore(inner);
    }
    record IndexData<K,V>(Function<V, String> keyGenerator, StoreBuilder<KeyValueStore<String, K>>idxStoreBuilder) {
    }
}
