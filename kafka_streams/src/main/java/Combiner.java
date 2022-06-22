import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Combiner {
    private static final Logger logger = LoggerFactory.getLogger(Combiner.class);

    public static void main(String[] args) {

        StoreBuilder<KeyValueStore<Long, Long>> resolvedInstruments =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("resolved-instruments"),
                        Serdes.Long(),
                        Serdes.Long()
                );

        StoreBuilder<KeyValueStore<String, String>> shelvedMissedData =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("shelved-data"),
                        Serdes.String(),
                        Serdes.String()
                );


        Topology topology = new Topology();

        topology
                .addSource("Waiting data", Serdes.Long().deserializer(), Serdes.String().deserializer(), "missed.instrument.data")
                .addSource("Resolved instruments", Serdes.Long().deserializer(), Serdes.Long().deserializer(), "seek.response")
                // add the WordCountProcessor node which takes the source processor as its upstream processor
                .addProcessor("Missed processor", Combiner.MissedProcessor::new, "Waiting data")
                .addProcessor("Instrument processor", Combiner.InstrumentProcessor::new, "Resolved instruments")

                // add the count store associated with the WordCountProcessor processor

                .addStateStore(shelvedMissedData, "Missed processor", "Instrument processor")
                .addStateStore(resolvedInstruments, "Missed processor", "Instrument processor")

                // add the sink processor node that takes Kafka topic "sink-topic" as output
                .addSink("Resolved data", "downstream.data", Serdes.String().serializer(), Serdes.String().serializer(), "Missed processor", "Instrument processor");

        TopologyDescription describe = topology.describe();

        logger.info("DAG {}", describe);

        Properties props = new Properties();
        props.setProperty("application.id", "combiner");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("commit.interval.ms", "100");
        props.setProperty("auto.commit.interval.ms", "100");
        props.setProperty("processing.guarantee", "exactly_once_v2");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }

    private static class InstrumentProcessor implements Processor<Long, Long, String, String> {
        private KeyValueStore<String, String> shelvedMissedData;
        private KeyValueStore<Long, Long> resolvedInstruments;

        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore("shelved-data");
            resolvedInstruments = context.getStateStore("resolved-instruments");
        }

        @Override
        public void process(Record<Long, Long> record) {
            logger.info("Resolved instrument {}:{}", record.key(), record.value());
            resolvedInstruments.put(record.key(), record.value());
            unshelve(record);
        }

        private void unshelve(Record<Long, Long> record) {
            try (KeyValueIterator<String, String> iterator = shelvedMissedData.prefixScan(record.key() + "_", Serdes.String().serializer())) {
                while (iterator.hasNext()) {
                    KeyValue<String, String> shelved = iterator.next();
                    logger.info("Unshevle message for instrument {}", shelved.value);
                    context.forward(enrichMessage(record, record.key(), shelved.value));
                    shelvedMissedData.delete(shelved.key);
                }
            }
        }
    }

    private static class MissedProcessor implements Processor<Long, String, String, String> {
        private KeyValueStore<String, String> shelvedMissedData;
        private KeyValueStore<Long, Long> resolvedInstruments;

        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore("shelved-data");
            resolvedInstruments = context.getStateStore("resolved-instruments");
        }

        @Override
        public void process(Record<Long, String> record) {
            Long instrument = resolvedInstruments.get(record.key());
            logger.info("Missed instrument message {}", record.value());
            if (instrument != null) {
                logger.info("Instrument was resolved {}", instrument);
                context.forward(enrichMessage(record, instrument, record.value()));
            } else {
                shelve(record);
            }
        }

        private void shelve(Record<Long, String> record) {
            String key = record.key() + "_" + record.value();
            logger.info("Shelve message with key {}", key);
            shelvedMissedData.put(key, record.value());
        }
    }

    public static Record<String, String> enrichMessage(Record record, long instrumentId, String value) {
        return new Record<>(String.valueOf(instrumentId), value, record.timestamp(), record.headers());
    }
}
