package com.limitium.gban.kscore.kstreamcore.processor;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.WrappedIndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.ProcessorPostInitListener;
import org.apache.kafka.streams.state.internals.WrapperSupplierFactoryAware;
import org.apache.kafka.streams.state.internals.WrapperValue;

import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
class WrappedIndexedKeyValueDecorator<S extends IndexedMeteredKeyValueStore<K, WrapperValue<W, V>>, K, V, W> extends WrappedKeyValueDecorator<S, K, V, W> implements WrappedIndexedKeyValueStore<K, V, W>, ProcessorPostInitListener {

    public WrappedIndexedKeyValueDecorator(S store, WrapperSupplierFactoryAware<K, V, W, ExtendedProcessorContext> wrapperSupplierFactoryAware, ExtendedProcessorContext pc) {
        super(store, wrapperSupplierFactoryAware, pc);
    }

    @Override
    public WrapperValue<W, V> getUnique(String indexName, String indexKey) {
        return getWrapped().getUnique(indexName, indexKey);
    }

    @Override
    public Stream<WrapperValue<W, V>> getNonUnique(String indexName, String indexKey) {
        return getWrapped().getNonUnique(indexName, indexKey);
    }


    @Override
    public void onPostInit(ProcessorContext processorContext) {
        getWrapped().onPostInit(processorContext);

    }

    @SuppressWarnings("unchecked")
    private S getWrapped() {
        return (S) wrapped();
    }
}
