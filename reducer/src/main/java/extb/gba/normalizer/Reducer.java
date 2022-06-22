package extb.gba.normalizer;

import extb.gba.instrument.normalizer.Topics;
import extb.gba.instrument.normalizer.messages.SeekRequest;
import extb.gba.instrument.normalizer.messages.UsStreetExecution;
import extb.gba.kscore.kstreamcore.KStreamInfraCustomizer;
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

    static final String STORE_SECURITY_ID_NAME = "throttled_securities_store";
    static final String SOURCE_NAME = "missed_instrument_source";
    static final String PROCESSOR_NAME = "throttler_transformer";
    static final String SINK_NAME = "seek_request_sink";

    @Override
    public void configureTopology(Topology topology) {

        //In memory only store to keep timestamp for key of first arrival
        StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder =
                Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(STORE_SECURITY_ID_NAME),
                                Serdes.String(),
                                Serdes.Long()
                        )
                        .withLoggingDisabled();


        //value deserializer isn't required, request can be generated from key only
        topology.addSource(SOURCE_NAME, Topics.INSTRUMENT_MISSED.keySerde.deserializer(), Topics.INSTRUMENT_MISSED.valueSerde.deserializer(), Topics.INSTRUMENT_MISSED.topic)
                // add the ReducerProcessor node which takes the source processor as its upstream processor
                .addProcessor(PROCESSOR_NAME, ReduceProcessor::new, SOURCE_NAME)
                // add the security id store associated with the ReducerProcessor processor
                .addStateStore(countStoreBuilder, PROCESSOR_NAME)
                // add the sink processor node to kafka topic
                .addSink(SINK_NAME, Topics.INSTRUMENT_SEEK_REQUEST.topic, Topics.INSTRUMENT_SEEK_REQUEST.keySerde.serializer(), Topics.INSTRUMENT_SEEK_REQUEST.valueSerde.serializer(), PROCESSOR_NAME);
    }

    public static class ReduceProcessor implements Processor<String, UsStreetExecution, String, SeekRequest>{
        public static final int THROTTLE_TIME_MS = 10_000;
        private KeyValueStore<String, Long> securityIdStore;
        private ProcessorContext<String, SeekRequest> context;

        @Override
        public void init(final ProcessorContext<String, SeekRequest> context) {
            this.context = context;
            //Storage cleanup, removes all records with insertedTimestamp + THROTTLE_TIME_MS < currentTime
            //should finish less than max.poll.interval.ms or will be kicked by consumer group
            context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (final KeyValueIterator<String, Long> iter = securityIdStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Long> entry = iter.next();
                        if (entry.value + THROTTLE_TIME_MS < timestamp) {
                            securityIdStore.delete(entry.key);
                            logger.info("{},removed", entry.key);
                        }
                    }
                }
            });
            securityIdStore = context.getStateStore(STORE_SECURITY_ID_NAME);

            System.err.println("INITED");
        }

        @Override
        public void process(final Record<String, UsStreetExecution> record) {
            logger.info("{}", record.key());
            if (securityIdStore.get(record.key()) == null) {
                logger.info("{},passed", record.key());
                context.forward(record.withValue(transformMissingInstrumentToSeekRequest(record)));
                securityIdStore.put(record.key(), record.timestamp());
            }
        }

        private SeekRequest transformMissingInstrumentToSeekRequest(Record<String, UsStreetExecution> record) {
            return new SeekRequest(record.value().securityId);
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }

    }
}
