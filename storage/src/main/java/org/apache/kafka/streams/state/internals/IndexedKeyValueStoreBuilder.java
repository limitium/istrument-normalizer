package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.Function;

public class IndexedKeyValueStoreBuilder<K, V> extends KeyValueStoreBuilder<K, V> {
    private final KeyValueBytesStoreSupplier storeSupplier;
    private final HashMap<String, Function<V, String>> uniqIndexes;

    public IndexedKeyValueStoreBuilder(KeyValueBytesStoreSupplier storeSupplier, Serde keySerde, Serde valueSerde, Time time) {
        super(storeSupplier, keySerde, valueSerde, time);
        this.storeSupplier = storeSupplier;
        this.uniqIndexes = new HashMap<>();
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

        uniqIndexes.put(indexName, keyGenerator);
        return this;
    }

    @Override
    public IndexedKeyValueStore<K, V> build() {
        return new IndexedMeteredKeyValueStore<>(
                uniqIndexes,
                maybeWrapCaching(maybeWrapLogging(storeSupplier.get())),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
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
}
