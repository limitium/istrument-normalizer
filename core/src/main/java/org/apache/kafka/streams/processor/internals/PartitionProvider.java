package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Objects;

/**
 * Retrieve partitions info from cluster
 */
public class PartitionProvider {
    Logger logger = LoggerFactory.getLogger(PartitionProvider.class);

    public static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final VarHandle HANDLE_STREAMS_PRODUCER;

    static {
        try {
            HANDLE_STREAMS_PRODUCER = MethodHandles.privateLookupIn(RecordCollectorImpl.class, LOOKUP).findVarHandle(RecordCollectorImpl.class, "streamsProducer", StreamsProducer.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    StreamsProducer streamsProducer;

    public PartitionProvider(ProcessorContext<?, ?> context) {
        if (context instanceof ProcessorContextImpl processorContext) {
            streamsProducer = (StreamsProducer) HANDLE_STREAMS_PRODUCER.get(((RecordCollectorImpl) processorContext.recordCollector()));
        } else {
            logger.error("Wrong context class `" + context.getClass().getName() + "`, expected `ProcessorContextImpl`. PartitionProvider will not work.");
        }
    }

    /**
     * Get partition count from the cluster
     *
     * @param topic
     * @return
     */
    public int getPartitionsCount(String topic) {
        List<PartitionInfo> partitionInfos = streamsProducer.partitionsFor(topic);
        Objects.requireNonNull(partitionInfos, "Partitions info can't be null for topic " + topic);
        return partitionInfos.size();
    }
}
