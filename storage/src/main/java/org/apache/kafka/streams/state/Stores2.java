package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.internals.*;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;
import org.rocksdb.Experimental;

import java.util.Objects;

public class Stores2 {
    /**
     * Creates a {@link IndexedKeyValueStoreBuilder} that can be used to build a {@link IndexedMeteredKeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     * <p>
     * To access to store from {@link org.apache.kafka.streams.processor.api.Processor#init(ProcessorContext)}
     * <pre> {@code
     *         @Override
     *         public void init(ProcessorContext<K, V> context) {
     *             Processor.super.init(context);
     *
     *             IndexedKeyValueStore<K, V> indexedStore = ((WrappedStateStore<IndexedKeyValueStore<K, V>, K, V>)context.getStateStore("store-name")).wrapped();
     *
     *             indexedStore.rebuildIndexes();
     *         }
     * }</pre>
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param <K>        key type
     * @param <V>        value type
     * @return an instance of a {@link IndexedKeyValueStoreBuilder} that can build a {@link IndexedMeteredKeyValueStore}
     */
    public static <K, V> IndexedKeyValueStoreBuilder<K, V> keyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                final Serde<K> keySerde,
                                                                                final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new IndexedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link WrappedKeyValueStoreBuilder} that can be used to build a {@link WrappedMeteredKeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     * <p>
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param wrapperSerde the wrapper serde to use/
     * @param wrapperSupplierFactory factory of wrapper supplier for each key to put
     *
     * @return an instance of a {@link WrappedKeyValueStoreBuilder} that can build a {@link WrappedMeteredKeyValueStore}
     * @param <K>        key type
     * @param <V>        value type
     * @param <W>        wrapper type
     * @param <PC>       processor context type
     */
    @Experimental("Do not use")
    public static <K, V, W, PC extends ProcessorContext> WrappedKeyValueStoreBuilder<K, V, W, PC> wrapKeyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                                                           final Serde<K> keySerde,
                                                                                                                           final Serde<V> valueSerde,
                                                                                                                           final Serde<W> wrapperSerde,
                                                                                                                           final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory
    ) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WrappedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, wrapperSerde, wrapperSupplierFactory, Time.SYSTEM);
    }

    /**
     * Creates a {@link WrappedIndexedKeyValueStoreBuilder} that can be used to build a {@link WrappedIndexedMeteredKeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     * <p>
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param wrapperSerde the wrapper serde to use/
     * @param wrapperSupplierFactory factory of wrapper supplier for each key to put
     *
     * @return an instance of a {@link WrappedIndexedKeyValueStoreBuilder} that can build a {@link WrappedIndexedMeteredKeyValueStore}
     * @param <K>        key type
     * @param <V>        value type
     * @param <W>        wrapper type
     * @param <PC>       processor context type
     */
    @Experimental("Do not use")
    public static <K, V, W, PC extends ProcessorContext> WrappedIndexedKeyValueStoreBuilder<K, V, W, PC> wrapIndexedKeyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                                                                         final Serde<K> keySerde,
                                                                                                                                         final Serde<V> valueSerde,
                                                                                                                                         final Serde<W> wrapperSerde,
                                                                                                                                         final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory
    ) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WrappedIndexedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, wrapperSerde, wrapperSupplierFactory, Time.SYSTEM);
    }

    /**
     * Creates a {@link WrappableKeyValueStoreBuilder} that holds {@link WrapperSupplierFactory} which will be used in {@link ExtendedProcessorContext}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     * <p>
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param wrapperSerde the wrapper serde to use/
     * @param wrapperSupplierFactory factory of wrapper supplier for each key to put
     *
     * @return an instance of a {@link WrappableKeyValueStoreBuilder} that can build a {@link WrappableMeteredKeyValueStore}
     * @param <K>        key type
     * @param <V>        value type
     * @param <W>        wrapper type
     * @param <PC>       processor context type
     */
    public static <K, V, W, PC extends ProcessorContext> WrappableKeyValueStoreBuilder<K, V, W, PC> wrappableKeyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                                                           final Serde<K> keySerde,
                                                                                                                           final Serde<V> valueSerde,
                                                                                                                           final Serde<W> wrapperSerde,
                                                                                                                           final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory
    ) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WrappableKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, wrapperSerde, wrapperSupplierFactory, Time.SYSTEM);
    }

    /**
     * Creates a {@link WrappableIndexedKeyValueStoreBuilder} that holds {@link WrapperSupplierFactory} which will be used in {@link ExtendedProcessorContext}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     * <p>
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param wrapperSerde the wrapper serde to use/
     * @param wrapperSupplierFactory factory of wrapper supplier for each key to put
     *
     * @return an instance of a {@link WrappableIndexedKeyValueStoreBuilder} that can build a {@link WrappableMeteredKeyValueStore}
     * @param <K>        key type
     * @param <V>        value type
     * @param <W>        wrapper type
     * @param <PC>       processor context type
     */
    public static <K, V, W, PC extends ProcessorContext> WrappableIndexedKeyValueStoreBuilder<K, V, W, PC> wrappableIndexedKeyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                                                                         final Serde<K> keySerde,
                                                                                                                                         final Serde<V> valueSerde,
                                                                                                                                         final Serde<W> wrapperSerde,
                                                                                                                                         final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory
    ) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WrappableIndexedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, wrapperSerde, wrapperSupplierFactory, Time.SYSTEM);
    }
}
