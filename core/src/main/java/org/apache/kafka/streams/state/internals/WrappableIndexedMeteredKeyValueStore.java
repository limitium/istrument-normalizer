package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;

import java.util.Map;
import java.util.function.Function;


@SuppressWarnings("rawtypes")
public class WrappableIndexedMeteredKeyValueStore<K, V, W, PC extends ProcessorContext> extends IndexedMeteredKeyValueStore<K, WrappedValue<W, V>> implements WrapperSupplierFactoryAware<K, V, W, PC> {
    private final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory;

    WrappableIndexedMeteredKeyValueStore(final Map<String, Function<WrappedValue<W, V>, String>> uniqIndexes,
                                         final Map<String, Function<WrappedValue<W, V>, String>> nonUniqIndexes,
                                         final KeyValueStore<Bytes, byte[]> inner,
                                         final String metricsScope,
                                         final Time time,
                                         final Serde<K> keySerde,
                                         final Serde<WrappedValue<W, V>> valueSerde,
                                         final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory) {
        super(uniqIndexes, nonUniqIndexes, inner, metricsScope, time, keySerde, valueSerde);
        this.wrapperSupplierFactory = wrapperSupplierFactory;
    }


    @Override
    public WrapperSupplierFactory<K, V, W, PC> getWrapperSupplierFactory() {
        return wrapperSupplierFactory;
    }
}
