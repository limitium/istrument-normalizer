package com.bnpparibas.gban.kscore.kstreamcore.stateless;

import com.bnpparibas.gban.kscore.kstreamcore.KSProcessor;
import com.bnpparibas.gban.kscore.kstreamcore.KSTopology;
import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopologyProvider {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class StatelessProcessor extends KSProcessor<Object, Object, Object, Object> {
        final Base statelessProcessorDefinition;

        public StatelessProcessor(Base statelessProcessorDefinition) {
            this.statelessProcessorDefinition = statelessProcessorDefinition;
        }


        @Override
        public void process(Record<Object, Object> record) {
            try {
                Record toSend = record;
                if (statelessProcessorDefinition instanceof Converter processorDefinition) {
                    toSend = processorDefinition.convert(record);
                }
                send(statelessProcessorDefinition.outputTopic(), toSend);
            } catch (Converter.ConvertException e) {
                sendToDLQ(record, e.getMessage(), e);
            }
        }
    }

    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    KStreamInfraCustomizer.KStreamKSTopologyBuilder defineStatelessProcessor(@Autowired(required = false) Base statelessProcessorDefinition) {
        if (statelessProcessorDefinition == null) {
            return null;
        }
        return topology -> {
            StreamPartitioner streamPartitioner = null;
            if (statelessProcessorDefinition instanceof Partitioner partitioner) {
                streamPartitioner = partitioner::partition;
            }
            topology.addProcessor(() -> new StatelessProcessor(statelessProcessorDefinition))
                    .withSource(statelessProcessorDefinition.inputTopic())
                    .withSink(new KSTopology.SinkDefinition(statelessProcessorDefinition.outputTopic(), null, streamPartitioner))
                    .withDLQ(statelessProcessorDefinition.dlq())
                    .done();
        };
    }
}
