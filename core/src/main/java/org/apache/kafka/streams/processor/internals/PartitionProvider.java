package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.processor.api.ProcessorContext;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Objects;

/**
 * Retrieve partitions info from cluster
 */
public class PartitionProvider {

    public static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final VarHandle HANDLE_STREAMS_PRODUCER;

    static {
        try {
            HANDLE_STREAMS_PRODUCER = MethodHandles
                    .privateLookupIn(RecordCollectorImpl.class, LOOKUP)
                    .findVarHandle(RecordCollectorImpl.class, "streamsProducer", StreamsProducer.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    final StreamsProducer streamsProducer;

    public PartitionProvider(ProcessorContext<?, ?> context) {
            streamsProducer = (StreamsProducer) HANDLE_STREAMS_PRODUCER.get(((RecordCollectorImpl) ((ProcessorContextImpl) context).recordCollector()));
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
