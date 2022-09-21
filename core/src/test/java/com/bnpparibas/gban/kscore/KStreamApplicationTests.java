package com.bnpparibas.gban.kscore;

import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "topic1",
                "topic2"
        },
        consumers = {"topic2"},
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KStreamApplicationTests.TopologyConfig.class
        }
)
class KStreamApplicationTests extends BaseKStreamApplicationTests {

    @Configuration
    public static class TopologyConfig {
        @Bean
        public static KStreamInfraCustomizer.KStreamDSLBuilder provideTopology() {
            return builder -> builder
                    .stream("topic1", Consumed.with(Serdes.String(), Serdes.String()))
                    .mapValues(s -> s + s)
                    .to("topic2", Produced.with(Serdes.String(), Serdes.String()));
        }
    }

    @Test
    void testDSLTopology() {
        String data = "qwe";

        send("topic1", data.getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom("topic2");

        assertEquals(data + data, new String(record.value(), Charset.defaultCharset()));


        data = "asdasd";

        send("topic1", data.getBytes(StandardCharsets.UTF_8));
        record = waitForRecordFrom("topic2");

        assertEquals(data + data, new String(record.value(), Charset.defaultCharset()));


        ensureEmptyTopic("topic2");
    }
}
