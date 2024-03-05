package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

@SuppressWarnings("rawtypes")
public abstract class InjectableKeyValueStoreBuilder<K, V, S extends KeyValueStore<K, V>> extends AbstractStoreBuilder<K, V, S> {


    public boolean injectable = false;

    protected final KeyValueBytesStoreSupplier storeSupplier;

    public InjectableKeyValueStoreBuilder(final KeyValueBytesStoreSupplier storeSupplier,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde,
                                final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        this.storeSupplier = storeSupplier;
    }


    /**
     * Add injectors to store, that allows direct store modification via source topic {application_name}.store-{store_name}-inject
     *
     * @return
     */
    public InjectableKeyValueStoreBuilder<K, V, S> addInjector() {
        this.injectable = true;
        return this;
    }

    public boolean isInjectable() {
        return this.injectable;
    }

    @Override
    public abstract S build();

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
