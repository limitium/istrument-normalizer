package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class KSInjectProcessor<KIn, VIn, KOut, VOut> extends KSProcessor<KIn, VIn, KOut, VOut> {
    Logger logger = LoggerFactory.getLogger(KSInjectProcessor.class);

    private final String storeName;
    KeyValueStore<KIn, VIn> kvStore;

    public KSInjectProcessor(@Nonnull StoreBuilder<?> storeBuilder) {
        this.storeName = storeBuilder.name();

    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
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
