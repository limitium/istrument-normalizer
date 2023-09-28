package com.limitium.gban.kscore.kstreamcore.processor;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseProcessor<KIn, VIn, KOut, VOut> implements org.apache.kafka.streams.processor.api.Processor<KIn, VIn, KOut, VOut> {

    Logger logger = LoggerFactory.getLogger(getClass());
    ExtendedProcessorContext<KIn, VIn, KOut, VOut> extendedProcessorContext;
    ExtendedProcessor<KIn, VIn, KOut, VOut> extendedProcessor;
    private final ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta;

    public BaseProcessor(ExtendedProcessor<KIn, VIn, KOut, VOut> extendedProcessor, ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta) {
        this.extendedProcessor = extendedProcessor;
        this.processorMeta = processorMeta;
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        extendedProcessorContext.updateIncomingRecord(record);
        try {
            extendedProcessor.process(record);
        } catch (Exception e) {
            logger.error("unhandled exception {}, for {}", e, record);
            extendedProcessorContext.sendToDLQ(record, e);
        }
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        extendedProcessorContext = new ExtendedProcessorContext<>(context, processorMeta);

        extendedProcessor.init(extendedProcessorContext);

        extendedProcessorContext.postProcessorInit();
    }

    @Override
    public void close() {
        extendedProcessor.close();
    }
}
