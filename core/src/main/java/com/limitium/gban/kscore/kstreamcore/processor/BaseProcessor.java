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
        logger.info("Incoming message {}-{} {}, updates extendedContext {}", extendedProcessorContext.getTopic(), extendedProcessorContext.getPartition(), record, extendedProcessorContext);
        extendedProcessorContext.updateIncomingRecord(record);
        try {
            extendedProcessor.process(record);
        } catch (Exception e) {
            logger.error("unhandled exception in {} {}, for {}", getName(), e, record);
            if (!extendedProcessorContext.hasDLQ()) {
                throw e;
            }
            extendedProcessorContext.sendToDLQ(record, e);
        }
    }

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        extendedProcessorContext = new ExtendedProcessorContext<>(context, processorMeta);
        logger.info("Init processor {}, new ExtendedContext created {}", getName(), extendedProcessorContext);

        extendedProcessor.init(extendedProcessorContext);

        extendedProcessorContext.postProcessorInit();
    }

    protected String getName() {
        return String.format("%s-%d", processorMeta.name, extendedProcessorContext.taskId().partition());
    }

    @Override
    public void close() {
        extendedProcessor.close();
    }
}
