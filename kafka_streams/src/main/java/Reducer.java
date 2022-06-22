import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
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

import java.time.Duration;
import java.util.Properties;

public class Reducer {

    //dedup with windowed store https://stackoverflow.com/questions/55803210/how-to-handle-duplicate-messages-using-kafka-streaming-dsl-functions
    private static final Logger logger = LoggerFactory.getLogger(Reducer.class);

    static class UniqProcessor implements Processor<Long, Long, Long, Long> {
        public static final int THROTTLE_TIME = 10_000;
        private KeyValueStore<Long, Long> kvStore;
        private ProcessorContext<Long, Long> context;

        @Override
        public void init(final ProcessorContext<Long, Long> context) {
            this.context = context;
            //should finish less than max.poll.interval.ms or will be kicked by consumer group
            context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (final KeyValueIterator<Long, Long> iter = kvStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<Long, Long> entry = iter.next();
                        if (entry.value + THROTTLE_TIME < timestamp) {
                            kvStore.delete(entry.key);
                            logger.info("delete key {}", entry.key);

//                        context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                        }
                    }
                }
            });
            kvStore = context.getStateStore("Passed-ids");
        }

        @Override
        public void process(final Record<Long, Long> record) {
            logger.info("got key {}", record.key());
            if (kvStore.get(record.key()) == null) {
                logger.info("added");
                context.forward(record);
            }
            kvStore.put(record.key(), record.timestamp());
        }

        @Override
        public void close() {
            // close any resources managed by this processor
            // Note: Do not close any StateStores as these are managed by the library
        }

    }

    public static void main(String[] args) {
        StoreBuilder<KeyValueStore<Long, Long>> countStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Passed-ids"),
                        Serdes.Long(),
                        Serdes.Long()
                ).withLoggingDisabled();

        Topology topology = new Topology();

        topology.addSource("Source MI", Serdes.Long().deserializer(), Serdes.Long().deserializer(), "missed.instrument")
                // add the WordCountProcessor node which takes the source processor as its upstream processor
                .addProcessor("Process MI reduce", UniqProcessor::new, "Source MI")
                // add the count store associated with the WordCountProcessor processor
                .addStateStore(countStoreBuilder, "Process MI reduce")
                // add the sink processor node that takes Kafka topic "sink-topic" as output
                .addSink("Sink MI throttled", "seek.request", Serdes.Long().serializer(), Serdes.Long().serializer(), "Process MI reduce");

        TopologyDescription describe = topology.describe();

        logger.info("DAG {}", describe);

        Properties props = new Properties();
        props.setProperty("application.id", "reducer");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("commit.interval.ms", "100");
        props.setProperty("auto.commit.interval.ms", "100");
        props.setProperty("processing.guarantee", "exactly_once_v2");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }
}
