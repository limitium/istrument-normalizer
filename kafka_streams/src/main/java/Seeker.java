import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Seeker {
    private static final Logger logger = LoggerFactory.getLogger(Seeker.class);
    public static final int RESPONSE_TIMEOUT = 5_000;

    public static void main(String[] args) {
        Serde<Long> longSerde = Serdes.Long();
        StreamsBuilder builder = new StreamsBuilder();

        builder
                .stream("seek.request", Consumed.with(longSerde, longSerde))
                .mapValues((readOnlyKey, value) -> Seeker.lookupInstrument(readOnlyKey))
                .peek((k, v) -> logger.info("Response: {}.{}", k, v))
                .to("seek.response", Produced.with(Serdes.Long(), Serdes.Long()));


        Topology topology = builder.build();
        Properties props = new Properties();
        props.setProperty("application.id", "seeker");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("commit.interval.ms", "100");
        props.setProperty("auto.commit.interval.ms", "100");
        props.setProperty("processing.guarantee", "exactly_once_v2");


        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

    }

    public static long lookupInstrument(long id) {
        if (id == 92) {
            return -1L;
        }
        return 1L;
    }
}
