package org.apache.kafka.streams.state.internals;


import jakarta.annotation.Nonnull;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class WrappedMeteredKeyValueStore<K, V, W, PC extends ProcessorContext> extends MeteredKeyValueStore<K, WrappedValue<W, V>> implements WrappedKeyValueStore<K, V, W>, ProcessorPostInitListener<PC> {

    private static final Logger logger = LoggerFactory.getLogger(WrappedMeteredKeyValueStore.class);

    //@todo: shouldn't be part of store, must be moved to some meta holder
    private final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory;
    private WrapperSupplier<K, V, W, ?> wrapperSupplier;

    WrappedMeteredKeyValueStore(
            final KeyValueStore<Bytes, byte[]> inner,
            final String metricsScope,
            final Time time,
            final Serde<K> keySerde,
            final Serde<WrappedValue<W, V>> valueSerde,
            final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory) {
        super(inner, metricsScope, time, keySerde, valueSerde);
        this.wrapperSupplierFactory = wrapperSupplierFactory;
    }

    @Override
    public W getWrapper(K key) {
        WrappedValue<W, V> wrappedValue = get(key);
        if (wrappedValue == null) {
            return null;
        }
        return wrappedValue.wrapper();
    }

    @Override
    public V getValue(K key) {
        WrappedValue<W, V> wrappedValue = get(key);
        if (wrappedValue == null) {
            return null;
        }
        return wrappedValue.value();
    }

    @Override
    public WrappedValue<W, V> delete(K key) {
        put(key, new WrappedValue<>(wrapperSupplier.generate(key, null), getValue(key)));
        return super.delete(key);
    }

    @Override
    public void putValue(K key, @Nonnull V value) {
        put(key, new WrappedValue<>(wrapperSupplier.generate(key, value), value));
    }

    @Override
    public void onPostInit(PC processorContext) {
        this.wrapperSupplier = wrapperSupplierFactory.create(this, processorContext);
    }
}
