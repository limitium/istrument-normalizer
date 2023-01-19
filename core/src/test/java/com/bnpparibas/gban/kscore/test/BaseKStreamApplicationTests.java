package com.bnpparibas.gban.kscore.test;

import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BaseKStreamApplicationTests {
    public static String[] consumerTopics;

    public static class CustomExecutionListener implements TestExecutionListener {

        @Override
        public void beforeTestClass(TestContext testContext) {
            consumerTopics = testContext.getTestClass().getAnnotation(KafkaTest.class).consumers();
        }
    }

    @DynamicPropertySource
    public static void testConsumerTopics(DynamicPropertyRegistry registry) {
        registry.add("test.consumer.topics", () -> consumerTopics);
    }

    @Configuration
    public static class BaseKafkaTestConfig {

        @Component
        public static class KafkaProducer {

            private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

            @Autowired
            private KafkaTemplate<byte[], byte[]> kafkaTemplate;

            public void send(String topic, byte[] key, byte[] value) {
                LOGGER.info("sending to topic='{}' payload='{}'", value, topic);
                kafkaTemplate.send(topic, key, value);
            }
        }

        @Component
        public class KafkaConsumer {
            private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

            @Value("${test.consumer.topics}")
            private String[] consumerTopics;

            private Map<String, LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>>> received =
                    new HashMap<>();

            @PostConstruct
            public void fillReceived() {
                for (String consumerTopic : consumerTopics) {
                    LOGGER.info("Create receiver for topic " + consumerTopic);
                    received.put(consumerTopic, new LinkedBlockingQueue<>());
                }
            }

            @KafkaListener(topics = "#{'${test.consumer.topics}'.split(',')}", groupId = "consumer1")
            public void receive(ConsumerRecord<byte[], byte[]> consumerRecord) {
                LOGGER.info("received payload='{}'", consumerRecord.toString());
                received.get(consumerRecord.topic()).add(consumerRecord);
            }

            public ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic) {
                assertTopic(topic);
                try {
                    int timeout = 10;
                    ConsumerRecord<byte[], byte[]> record =
                            received.get(topic).poll(timeout, TimeUnit.SECONDS);
                    assertNotNull(record, "Topic `" + topic + "` is empty after "+ timeout +"s");
                    return record;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            public void ensureEmpty(String topic) {
                assertTopic(topic);
                try {
                    ConsumerRecord<byte[], byte[]> mustBeEmpty =
                            received.get(topic).poll(100, TimeUnit.MILLISECONDS);
                    assertNull(mustBeEmpty, "Topic `" + topic + "` is not empty");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            private void assertTopic(String topic) {
                if (!received.containsKey(topic)) {
                    throw new RuntimeException(
                            "Topic `" + topic + "` wasn't set as consumer in @KafkaTest annotation");
                }
            }
        }
    }

    @Autowired
    private BaseKafkaTestConfig.KafkaConsumer consumer;

    @Autowired
    private BaseKafkaTestConfig.KafkaProducer producer;

    /**
     * Sends key and value in {@link Topic#topic}
     *
     * @param topic
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     */
    protected <K, V> void send(Topic<K, V> topic, K key, V value) {
        send(
                topic.topic,
                topic.keySerde.serializer().serialize(topic.topic, key),
                topic.valueSerde.serializer().serialize(topic.topic, value));
    }

    /**
     * Sends message into `topicName` instead of {@link Topic#topic}
     *
     * @param topic
     * @param topicName Overrides topic if it has `*` as a subscription pattern in {@link
     *                  Topic#topic}
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     */
    protected <K, V> void send(Topic<K, V> topic, String topicName, K key, V value) {
        send(
                topicName,
                topic.keySerde.serializer().serialize(topicName, key),
                topic.valueSerde.serializer().serialize(topicName, value));
    }

    protected void send(String topic, byte[] value) {
        producer.send(topic, null, value);
    }

    protected void send(String topic, byte[] key, byte[] value) {
        producer.send(topic, key, value);
    }

    /**
     * Wait for a single message in a concrete topic
     *
     * @param topic
     * @param <K>
     * @param <V>
     * @return
     */
    protected <K, V> ConsumerRecord<K, V> waitForRecordFrom(Topic<K, V> topic) {
        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom(topic.topic);

        return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                topic.keySerde.deserializer().deserialize(record.topic(), record.key()),
                topic.valueSerde.deserializer().deserialize(record.topic(), record.value()));
    }

    protected ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic) {
        return consumer.waitForRecordFrom(topic);
    }

    protected <K, V> void ensureEmptyTopic(Topic<K, V> topic) {
        consumer.ensureEmpty(topic.topic);
    }

    protected void ensureEmptyTopic(String topic) {
        consumer.ensureEmpty(topic);
    }
}
