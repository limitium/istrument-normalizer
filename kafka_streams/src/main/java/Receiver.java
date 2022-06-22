import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Receiver {
    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    static class InstrSvc {
        public static long getInstrumentId(String msg) {
            return Integer.parseInt(msg.split("\\|")[1]);
        }

        public static Long getInstrument(String msg) {
            long instId = getInstrumentId(msg);
            return instId < 90 ? instId : null;
        }
    }

    public static void main(String[] args) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        builder
                .stream("upstream.data", Consumed.with(stringSerde, stringSerde))
                .mapValues(s -> KeyValue.pair(InstrSvc.getInstrument(s), s))
                .split()
                .branch((k, p) -> p.key == null,
                        Branched.withConsumer(ks -> {
                                    KStream<Long, String> missedStream = ks
                                            .map(new KeyValueMapper<String, KeyValue<Long, String>, KeyValue<Long, String>>() {
                                                @Override
                                                public KeyValue<Long, String> apply(String key, KeyValue<Long, String> value) {
                                                    return KeyValue.pair(InstrSvc.getInstrumentId(value.value), value.value);
                                                }
                                            }, Named.as("instrument-as-a-key"))
                                            .peek((k, v) -> logger.info("To missed: {}.{}", k, v));
                                    missedStream
                                            .mapValues((readOnlyKey, value) -> readOnlyKey)
                                            .to("missed.instrument", Produced.with(Serdes.Long(), Serdes.Long()));
                                    missedStream
                                            .to("missed.instrument.data", Produced.with(Serdes.Long(), stringSerde));
                                }
                        )
                )
                .defaultBranch(Branched.withConsumer(ks -> ks
                                .mapValues((k, p) -> p.value + "I:" + p.key)
//                        .peek((k, v) -> logger.info("To downstream: {}.{}", k, v))
                                .to("downstream.data", Produced.with(stringSerde, stringSerde))
                ));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.setProperty("application.id", "receiver");
        props.setProperty("bootstrap.servers", "localhost:9092");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

    }
}
