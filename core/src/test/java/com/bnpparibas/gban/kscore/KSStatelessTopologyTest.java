package com.bnpparibas.gban.kscore;

import com.bnpparibas.gban.kscore.kstreamcore.DLQ;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import com.bnpparibas.gban.kscore.kstreamcore.stateless.Converter;
import com.bnpparibas.gban.kscore.kstreamcore.stateless.Partitioner;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {"test.in.ss.1"},
        consumers = {
                "test.out.ss.1",
                "test.out.dlq.1"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSStatelessTopologyTest.ProcessorConfig.class
        })
class KSStatelessTopologyTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, Integer> SOURCE = new Topic<>("test.in.ss.1", Serdes.Integer(), Serdes.Integer());
    public static final Topic<Integer, Integer> SINK1 = new Topic<>("test.out.ss.1", Serdes.Integer(), Serdes.Integer());
    public static final Topic<Integer, String> DLQ_TOPIC = new Topic<>("test.out.dlq.1", Serdes.Integer(), Serdes.String());
    public static final DLQ<Integer, Integer, String> DLQ_MAIN = new DLQ<>(DLQ_TOPIC, (exceptionId, failed, fromTopic, partition, offset, errorMessage, exception) -> failed.withValue(errorMessage));

    @Configuration
    public static class ProcessorConfig {

        interface StatelessProc extends Converter<Integer, Integer, Integer, Integer, String>, Partitioner<Integer, Integer, Integer, Integer, String> {
        }

        @Bean
        public static StatelessProc stateless() {
            return new StatelessProc() {
                @Override
                public Topic<Integer, Integer> inputTopic() {
                    return SOURCE;
                }

                @Override
                public Topic<Integer, Integer> outputTopic() {
                    return SINK1;
                }

                @Override
                public DLQ<Integer, Integer, String> dlq() {
                    return DLQ_MAIN;
                }

                @Override
                public Record<Integer, Integer> convert(Record<Integer, Integer> toConvert) throws ConvertException {
                    if (toConvert.value() < 1) {
                        throw new ConvertException("negative");
                    }
                    return toConvert.withValue(toConvert.value() * 2);
                }

                @Override
                public int partition(String topic, Integer key, Integer value, int numPartitions) {
                    int i = key % 2;
                    return i;
                }
            };

        }
    }

    @Test
    void testConverter() {
        send(SOURCE, 1, 2);
        ConsumerRecord<Integer, Integer> out = waitForRecordFrom(SINK1);

        assertEquals(1, out.key());
        assertEquals(4, out.value());
    }

    @Test
    void testPartitioner() {
        send(SOURCE, 2, 1);
        ConsumerRecord<Integer, Integer> out = waitForRecordFrom(SINK1);

        assertEquals(2, out.key());
        assertEquals(0, out.partition());

        send(SOURCE, 3, 1);
        out = waitForRecordFrom(SINK1);

        assertEquals(3, out.key());
        assertEquals(1, out.partition());
    }

    @Test
    void testDQL() {
        send(SOURCE, 4, -1);
        ConsumerRecord<Integer, String> out = waitForRecordFrom(DLQ_TOPIC);

        assertEquals(4, out.key());
        assertEquals("negative", out.value());
    }
}
