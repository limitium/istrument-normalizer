package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;

@SuppressWarnings("rawtypes")
public class WrappableMeteredKeyValueStore<K, V, W, PC extends ProcessorContext> extends MeteredKeyValueStore<K, WrapperValue<W, V>> implements WrapperSupplierFactoryAware<K, V, W, PC> {
    private final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory;

    WrappableMeteredKeyValueStore(
            final KeyValueStore<Bytes, byte[]> inner,
            final String metricsScope,
            final Time time,
            final Serde<K> keySerde,
            final Serde<WrapperValue<W, V>> valueSerde,
            final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory) {
        super(inner, metricsScope, time, keySerde, valueSerde);
        this.wrapperSupplierFactory = wrapperSupplierFactory;
    }


    @Override
    public WrapperSupplierFactory<K, V, W, PC> getWrapperSupplierFactory() {
        return wrapperSupplierFactory;
    }
}
