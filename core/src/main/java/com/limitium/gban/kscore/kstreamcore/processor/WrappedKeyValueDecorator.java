package com.limitium.gban.kscore.kstreamcore.processor;

import jakarta.annotation.Nonnull;
import org.apache.kafka.streams.processor.internals.KeyValueReadWriteDecorator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier;
import org.apache.kafka.streams.state.internals.WrapperSupplierFactoryAware;
import org.apache.kafka.streams.state.internals.WrappedValue;

@SuppressWarnings("rawtypes")
class WrappedKeyValueDecorator<S extends KeyValueStore<K, WrappedValue<W, V>>, K, V, W> extends KeyValueReadWriteDecorator<K, WrappedValue<W, V>> implements WrappedKeyValueStore<K, V, W> {
    protected final WrapperSupplier<K, V, W, ?> wrapperSupplier;

    public WrappedKeyValueDecorator(S store, WrapperSupplierFactoryAware<K, V, W, ExtendedProcessorContext> wrapperSupplierFactoryAware, ExtendedProcessorContext pc) {
        super(store);
        wrapperSupplier = wrapperSupplierFactoryAware.getWrapperSupplierFactory().create(this, pc);
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
    public void putValue(K key, @Nonnull V value) {
        put(key, new WrappedValue<>(wrapperSupplier.generate(key, value), value));
    }

    @Override
    public WrappedValue<W, V> delete(K key) {
        put(key, new WrappedValue<>(wrapperSupplier.generate(key, null), getValue(key)));
        return super.delete(key);
    }
}
