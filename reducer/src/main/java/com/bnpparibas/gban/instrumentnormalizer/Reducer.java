package com.limitium.gban.instrumentnormalizer;

import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class Reducer implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Reducer.class);

    static final String STORE_SECURITY_ID_NAME = "throttled_store";
    static final String SOURCE_NAME = "main_source";
    static final String PROCESSOR_NAME = "throttler_processor";
    static final String SINK_NAME = "main_sink";

    @Override
    public void configureTopology(Topology topology) {

        //In memory only store to keep timestamp for key of first arrival
        StoreBuilder<KeyValueStore<String, Long>> lastSentTimeForKeyStore =
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(STORE_SECURITY_ID_NAME),
                                Serdes.String(),
                                Serdes.Long()
                        )
                        .withLoggingDisabled();


        //value deserializer isn't required, request can be generated from key only
        topology.addSource(SOURCE_NAME, Topics.REDUCE_LOOKUP_INSTRUMENT.keySerde.deserializer(), Topics.REDUCE_LOOKUP_INSTRUMENT.valueSerde.deserializer(), Topics.REDUCE_LOOKUP_INSTRUMENT.topic)
                // add the ReducerProcessor node which takes the source processor as its upstream processor
                .addProcessor(PROCESSOR_NAME, ReduceProcessor::new, SOURCE_NAME)
                // add the security id store associated with the ReducerProcessor processor
                .addStateStore(lastSentTimeForKeyStore, PROCESSOR_NAME)
                // add the sink processor node to kafka topic
                .addSink(SINK_NAME, Topics.LOOKUP_INSTRUMENT.topic, Topics.LOOKUP_INSTRUMENT.keySerde.serializer(), Topics.LOOKUP_INSTRUMENT.valueSerde.serializer(), PROCESSOR_NAME);
    }

    //@todo: migrate from FBLookupInstrument to generic byte[]
    public static class ReduceProcessor implements Processor<String, FBLookupInstrument, String, FBLookupInstrument> {
        public static final int THROTTLE_TIME_MS = 10_000;
        private KeyValueStore<String, Long> lastSentTimeForKeyStore;
        private ProcessorContext<String, FBLookupInstrument> context;

        @Override
        public void init(final ProcessorContext<String, FBLookupInstrument> context) {
            this.context = context;
            //Storage cleanup, removes all records with insertedTimestamp + THROTTLE_TIME_MS < currentTime
            //should finish less than max.poll.interval.ms or will be kicked by consumer group
            context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (final KeyValueIterator<String, Long> iter = lastSentTimeForKeyStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Long> entry = iter.next();
                        if (entry.value + THROTTLE_TIME_MS < timestamp) {
                            lastSentTimeForKeyStore.delete(entry.key);
                            logger.info("{},removed", entry.key);
                        }
                    }
                }
            });
            lastSentTimeForKeyStore = context.getStateStore(STORE_SECURITY_ID_NAME);

            System.err.println("INITED");
        }

        @Override
        public void process(final Record<String, FBLookupInstrument> record) {
            logger.info("{}", record.key());
            if (lastSentTimeForKeyStore.get(record.key()) == null) {
                logger.info("{},passed", record.key());

                context.forward(record);

                lastSentTimeForKeyStore.put(record.key(), record.timestamp());
            }
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }

    }
}
