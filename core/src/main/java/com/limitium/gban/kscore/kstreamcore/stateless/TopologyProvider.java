package com.limitium.gban.kscore.kstreamcore.stateless;

import com.limitium.gban.kscore.kstreamcore.KSTopology;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopologyProvider {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class StatelessProcessor implements ExtendedProcessor<Object, Object, Object, Object> {
        final Base statelessProcessorDefinition;
        private ExtendedProcessorContext<Object, Object, Object, Object> context;

        public StatelessProcessor(Base statelessProcessorDefinition) {
            this.statelessProcessorDefinition = statelessProcessorDefinition;
        }

        @Override
        public void init(ExtendedProcessorContext<Object, Object, Object, Object> context) {
            this.context = context;
        }

        @Override
        public void process(Record<Object, Object> record) {
            try {
                Record toSend = record;
                if (statelessProcessorDefinition instanceof Converter processorDefinition) {
                    toSend = processorDefinition.convert(record);
                    if (toSend == null) {
                        return;
                    }
                }
                context.send(statelessProcessorDefinition.outputTopic(), toSend);
            } catch (Converter.ConvertException e) {
                context.sendToDLQ(record, e.getMessage(), e);
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

            KSTopology.ProcessorDefinition processorDefinition = topology.addProcessor(() -> new StatelessProcessor(statelessProcessorDefinition))
                    .withSource(statelessProcessorDefinition.inputTopic())
                    .withSink(new KSTopology.SinkDefinition(statelessProcessorDefinition.outputTopic(), null, streamPartitioner));

            if (statelessProcessorDefinition.dlq() != null) {
                processorDefinition.withDLQ(statelessProcessorDefinition.dlq());
            }

            processorDefinition.done();
        };
    }
}
