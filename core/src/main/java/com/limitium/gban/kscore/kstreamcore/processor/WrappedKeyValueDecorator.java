package com.limitium.gban.kscore.kstreamcore.processor;

import jakarta.annotation.Nonnull;
import org.apache.kafka.streams.processor.internals.KeyValueReadWriteDecorator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier;
import org.apache.kafka.streams.state.internals.WrapperSupplierFactoryAware;
import org.apache.kafka.streams.state.internals.WrapperValue;

@SuppressWarnings("rawtypes")
class WrappedKeyValueDecorator<S extends KeyValueStore<K, WrapperValue<W, V>>, K, V, W> extends KeyValueReadWriteDecorator<K, WrapperValue<W, V>> implements WrappedKeyValueStore<K, V, W> {
    protected final WrapperSupplier<K, V, W, ?> wrapperSupplier;

    public WrappedKeyValueDecorator(S store, WrapperSupplierFactoryAware<K, V, W, ExtendedProcessorContext> wrapperSupplierFactoryAware, ExtendedProcessorContext pc) {
        super(store);
        wrapperSupplier = wrapperSupplierFactoryAware.getWrapperSupplierFactory().create(this, pc);
    }

    @Override
    public W getWrapper(K key) {
        WrapperValue<W, V> wrapperValue = get(key);
        if (wrapperValue == null) {
            return null;
        }
        return wrapperValue.wrapper();
    }

    @Override
    public V getValue(K key) {
        WrapperValue<W, V> wrapperValue = get(key);
        if (wrapperValue == null) {
            return null;
        }
        return wrapperValue.value();
    }

    @Override
    public void putValue(K key, @Nonnull V value) {
        put(key, new WrapperValue<>(wrapperSupplier.generate(key, value), value));
    }

    @Override
    public WrapperValue<W, V> delete(K key) {
        put(key, new WrapperValue<>(wrapperSupplier.generate(key, null), getValue(key)));
        return super.delete(key);
    }
}
