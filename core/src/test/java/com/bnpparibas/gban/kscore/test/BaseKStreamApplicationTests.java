package com.bnpparibas.gban.kscore.test;

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
            private Map<String, LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>>> received = new HashMap<>();

            @PostConstruct
            public void fillReceived() {
                for (String consumerTopic : consumerTopics) {
                    LOGGER.info("Create receiver for topic " + consumerTopic);
                    received.put(consumerTopic, new LinkedBlockingQueue<>());
                }
            }

            @KafkaListener(topics = "${test.consumer.topics}", groupId = "consumer1")
            public void receive(ConsumerRecord<byte[], byte[]> consumerRecord) {
                LOGGER.info("received payload='{}'", consumerRecord.toString());
                received.get(consumerRecord.topic()).add(consumerRecord);
            }

            public ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic) {
                assertTopic(topic);
                try {
                    ConsumerRecord<byte[], byte[]> record = received.get(topic).poll(10, TimeUnit.SECONDS);
                    assertNotNull(record);
                    return record;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            public void ensureEmpty(String topic) {
                assertTopic(topic);
                try {
                    ConsumerRecord<byte[], byte[]> mustBeEmpty = received.get(topic).poll(100, TimeUnit.MILLISECONDS);
                    assertNull(mustBeEmpty);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            private void assertTopic(String topic) {
                if (!received.containsKey(topic)) {
                    throw new RuntimeException("Topic `" + topic + "` wasn't set as consumer in @KafkaTest");
                }
            }

        }
    }

    @Autowired
    private BaseKafkaTestConfig.KafkaConsumer consumer;

    @Autowired
    private BaseKafkaTestConfig.KafkaProducer producer;

    protected void send(String topic, byte[] value) {
        producer.send(topic, null, value);
    }

    protected void send(String topic, byte[] key, byte[] value) {
        producer.send(topic, null, value);
    }

    protected ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic) {
        return consumer.waitForRecordFrom(topic);
    }

    protected void ensureEmptyTopic(String topic) {
        consumer.ensureEmpty(topic);
    }
}
