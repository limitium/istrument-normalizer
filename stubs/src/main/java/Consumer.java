import extb.gba.instrument.normalizer.Topics;
import extb.gba.instrument.normalizer.messages.UsStreetExecution;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "stub-subscriber-app");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        KafkaConsumer<Long, UsStreetExecution> consumer = new KafkaConsumer<>(props, Topics.INSTRUMENT_ENRICHED.keySerde.deserializer(), Topics.INSTRUMENT_ENRICHED.valueSerde.deserializer());

        String topic = Topics.INSTRUMENT_ENRICHED.topic;

        consumer.subscribe(List.of(topic));
        logger.info("Subscribed to topic {}", topic);

        while (true) {
            ConsumerRecords<Long, UsStreetExecution> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<Long, UsStreetExecution> record : records) {
                logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }
        }
    }
}