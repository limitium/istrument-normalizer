package com.bnpparibas.gban.kscore;

import com.bnpparibas.gban.kscore.kstreamcore.KSProcessor;
import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {"test.in.topic.1"},
        consumers = {
                "test.out.topic.1",
                "test.out.topic.2"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSTopologyTest.TopologyConfig.class
        })
class KSTopologyTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, Long> SOURCE = new Topic<>("test.in.topic.1", Serdes.Integer(), Serdes.Long());
    public static final Topic<String, String> SINK1 = new Topic<>("test.out.topic.1", Serdes.String(), Serdes.String());
    public static final Topic<String, String> SINK2 = new Topic<>("test.out.topic.2", Serdes.String(), Serdes.String());

    @Configuration
    public static class TopologyConfig {
        public static class TestProcessor extends KSProcessor<Integer, Long, String, String> {

            private KeyValueStore<Integer, Long> inMemKv;

            @Override
            public void init(ProcessorContext<String, String> context) {
                super.init(context);
                inMemKv = context.getStateStore("in_mem_kv");
            }

            @Override
            public void process(Record<Integer, Long> record) {
                inMemKv.putIfAbsent(record.key(), 0L);
                inMemKv.put(record.key(), inMemKv.get(record.key()) + record.value());

                send(record.value() % 2 != 0 ? SINK1 : SINK2, record
                        .withKey("k_" + record.key())
                        .withValue("v_" + inMemKv.get(record.key()))
                );
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                StoreBuilder<KeyValueStore<Integer, Long>> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("in_mem_kv"), Serdes.Integer(), Serdes.Long());

                builder
                        .addProcessor(TestProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .withSink(SINK1)
                        .withSink(SINK2)
                        .done();
            };

        }
    }

    @Test
    void testTopology() {
        send(SOURCE, 1, 1L);
        ConsumerRecord<String, String> out = waitForRecordFrom(SINK1);

        assertEquals("k_1", out.key());
        assertEquals("v_1", out.value());

        send(SOURCE, 1, 2L);
        out = waitForRecordFrom(SINK2);

        assertEquals("k_1", out.key());
        assertEquals("v_3", out.value());
    }
}
