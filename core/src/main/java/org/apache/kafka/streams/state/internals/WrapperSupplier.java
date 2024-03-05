package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.WrappedKeyValueStore;

/**
 * Provides wrapper header for each {@link WrappedKeyValueStore#putValue(Object, Object)} call.
 * Has access to {@link WrappedKeyValueStore} instance and {@link ProcessorContext}.
 * Single instance is created per {@link org.apache.kafka.streams.processor.internals.Task}
 *
 * @param <K>  key type
 * @param <V>  value type
 * @param <W>  wrapper type
 * @param <PC> process context type
 */
public abstract class WrapperSupplier<K, V, W, PC extends ProcessorContext> {
    protected WrappedKeyValueStore<K, V, W> store;
    protected PC context;

    public WrapperSupplier(WrappedKeyValueStore<K, V, W> store, PC context) {
        this.store = store;
        this.context = context;
    }

    /**
     * Generates wrapper based on key, value and store state.
     * In case of delete value will be null.
     *
     * @param key
     * @param value
     * @return
     */
    abstract public W generate(K key, V value);

    /**
     * Factory of {@link WrapperSupplier} use in {@link WrappedKeyValueStoreBuilder}
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param <W>  wrapper type
     * @param <PC> process context type
     */
    @SuppressWarnings("rawtypes")
    public interface WrapperSupplierFactory<K, V, W, PC extends ProcessorContext> {
        WrapperSupplier<K, V, W, PC> create(WrappedKeyValueStore<K, V, W> store, PC context);
    }
}
