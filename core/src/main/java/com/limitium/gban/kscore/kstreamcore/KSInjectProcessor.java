package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class KSInjectProcessor<KIn, VIn, KOut, VOut> implements ExtendedProcessor<KIn, VIn, KOut, VOut> {
    Logger logger = LoggerFactory.getLogger(KSInjectProcessor.class);

    private final String storeName;
    KeyValueStore<KIn, VIn> kvStore;

    public KSInjectProcessor(@Nonnull StoreBuilder<?> storeBuilder) {
        this.storeName = storeBuilder.name();
    }

    @Override
    public void init(ExtendedProcessorContext<KIn, VIn, KOut, VOut> context) {
        kvStore = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        try {
            kvStore.put(record.key(), record.value());
            logger.info("injected,{}:{}", storeName, record.key());
        } catch (RuntimeException ex) {
            logger.error("inject_failed,{}:{}", storeName, record.key(), ex);
        }
    }
}
