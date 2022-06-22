package extb.gba.normalizer;

import extb.gba.instrument.normalizer.Topics;
import extb.gba.instrument.normalizer.messages.InstrumentDefinition;
import extb.gba.instrument.normalizer.messages.UsStreetExecution;
import extb.gba.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
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

@Component
public class Combiner implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Combiner.class);

    public static final String SOURCE_MISSING_INSTRUMENT = "missed_instrument_source";
    public static final String SOURCE_RESOLVED_INSTRUMENT = "resolved_instrument_source";
    public static final String STORE_RESOLVED_INSTRUMENT_NAME = "resolved_instrument_store";
    public static final String STORE_SHELVED_DATA_NAME = "shelved_messages_store";
    public static final String PROCESSOR_MISSED_INSTRUMENT = "missed_instrument_processor";
    public static final String PROCESSOR_RESOLVED_INSTRUMENT = "resolved_instrument_processor";
    public static final String SINK_ENRICHED_INSTRUMENT = "enriched_messages_sink";

    @Override
    public void configureTopology(Topology topology) {
        StoreBuilder<KeyValueStore<String, InstrumentDefinition>> resolvedInstruments =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_RESOLVED_INSTRUMENT_NAME),
                        Serdes.String(),
                        Topics.INSTRUMENT_UPDATED.valueSerde
                );

        StoreBuilder<KeyValueStore<String, UsStreetExecution>> shelvedMissedData =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_SHELVED_DATA_NAME),
                        Serdes.String(),
                        Topics.INSTRUMENT_MISSED.valueSerde
                );


        topology
                .addSource(SOURCE_MISSING_INSTRUMENT, Topics.INSTRUMENT_MISSED.keySerde.deserializer(), Topics.INSTRUMENT_MISSED.valueSerde.deserializer(), Topics.INSTRUMENT_MISSED.topic)
                .addSource(SOURCE_RESOLVED_INSTRUMENT, Topics.INSTRUMENT_UPDATED.keySerde.deserializer(), Topics.INSTRUMENT_UPDATED.valueSerde.deserializer(), Topics.INSTRUMENT_UPDATED.topic)
                // add two processors one per source
                .addProcessor(PROCESSOR_MISSED_INSTRUMENT, Combiner.MissedProcessor::new, SOURCE_MISSING_INSTRUMENT)
                .addProcessor(PROCESSOR_RESOLVED_INSTRUMENT, Combiner.InstrumentProcessor::new, SOURCE_RESOLVED_INSTRUMENT)

                // add stores to both processors
                .addStateStore(shelvedMissedData, PROCESSOR_MISSED_INSTRUMENT, PROCESSOR_RESOLVED_INSTRUMENT)
                .addStateStore(resolvedInstruments, PROCESSOR_MISSED_INSTRUMENT, PROCESSOR_RESOLVED_INSTRUMENT)

                // add sink to both processors
                .addSink(SINK_ENRICHED_INSTRUMENT, Topics.INSTRUMENT_ENRICHED.topic, Topics.INSTRUMENT_ENRICHED.keySerde.serializer(), Topics.INSTRUMENT_ENRICHED.valueSerde.serializer(), PROCESSOR_MISSED_INSTRUMENT, PROCESSOR_RESOLVED_INSTRUMENT);

    }

    private static class InstrumentProcessor implements Processor<String, InstrumentDefinition, Long, UsStreetExecution> {
        private KeyValueStore<String, UsStreetExecution> shelvedMissedData;
        private KeyValueStore<String, InstrumentDefinition> resolvedInstruments;
        private ProcessorContext<Long, UsStreetExecution> context;

        @Override
        public void init(ProcessorContext<Long, UsStreetExecution> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore(STORE_SHELVED_DATA_NAME);
            resolvedInstruments = context.getStateStore(STORE_RESOLVED_INSTRUMENT_NAME);
        }

        @Override
        public void process(Record<String, InstrumentDefinition> record) {
            logger.info("Instr:{}:{}", record.key(), record.value().instrumentId);
            resolvedInstruments.put(record.key(), record.value());
            unshelve(record);
        }

        private void unshelve(Record<String, InstrumentDefinition> record) {
            try (KeyValueIterator<String, UsStreetExecution> iterator = shelvedMissedData.prefixScan(record.key() + "_", Serdes.String().serializer())) {
                while (iterator.hasNext()) {
                    KeyValue<String, UsStreetExecution> shelved = iterator.next();

                    InstrumentDefinition instrument = record.value();
                    UsStreetExecution instrumental = shelved.value;

                    logger.info("{},{}->{}", instrumental.executionId, instrumental.securityId, instrument.instrumentId);
                    context.forward(enrichMessage(record, instrumental, instrument));

                    shelvedMissedData.delete(shelved.key);
                }
            }
        }
    }

    private static class MissedProcessor implements Processor<String, UsStreetExecution, Long, UsStreetExecution> {
        private KeyValueStore<String, UsStreetExecution> shelvedMissedData;
        private KeyValueStore<String, InstrumentDefinition> resolvedInstruments;
        private ProcessorContext<Long, UsStreetExecution> context;

        @Override
        public void init(ProcessorContext<Long, UsStreetExecution> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore(STORE_SHELVED_DATA_NAME);
            resolvedInstruments = context.getStateStore(STORE_RESOLVED_INSTRUMENT_NAME);
        }

        @Override
        public void process(Record<String, UsStreetExecution> record) {
            logger.info("Missed:{}", record.key());

            InstrumentDefinition instrument = resolvedInstruments.get(record.key());
            if (instrument != null) {
                UsStreetExecution instrumental = record.value();

                logger.info("{},{}->{}", instrumental.executionId, instrumental.securityId, instrument.instrumentId);
                context.forward(enrichMessage(record, instrumental, instrument));
            } else {
                shelve(record);
            }
        }

        private void shelve(Record<String, UsStreetExecution> record) {
            //todo: add snowflake id generator
            String key = record.key() + "_" + System.nanoTime();
            logger.info("Shelved:{}", key);
            shelvedMissedData.put(key, record.value());
        }
    }

    public static Record<Long, UsStreetExecution> enrichMessage(Record record, UsStreetExecution instrumental, InstrumentDefinition instrument) {
        instrumental.setInstrumentId(instrument.instrumentId);
        return new Record<>(instrument.instrumentId, instrumental, record.timestamp(), record.headers());
    }
}
