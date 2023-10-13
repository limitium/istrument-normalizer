package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.api.ProcessorContext;

@SuppressWarnings("rawtypes")
public interface WrapperSupplierFactoryAware<K, V, W, PC extends ProcessorContext> {
    WrapperSupplier.WrapperSupplierFactory<K, V, W, PC> getWrapperSupplierFactory();
}
