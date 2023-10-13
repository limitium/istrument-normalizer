package com.limitium.gban.kscore.audit;

import com.limitium.gban.kscore.KStreamApplication;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.limitium.gban.kscore.kstreamcore.Topic;
import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import com.limitium.gban.kscore.test.BaseKStreamApplicationTests;
import com.limitium.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WrapperValue;
import org.apache.kafka.streams.state.internals.WrapperValueSerde;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@KafkaTest(topics = {"test.in.topic.1"}, consumers = {"core-app.store-kv-changelog"}, configs = {
        KStreamApplication.class,
        BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
        AuditWrapperTests.TopologyConfig.class
})
class AuditWrapperTests extends BaseKStreamApplicationTests {
    public static final Topic<Long, String> SOURCE = new Topic<>("test.in.topic.1", Serdes.Long(), Serdes.String());
    public static final Topic<Long, WrapperValue<Audit, String>> CHANGELOG = new Topic<>("core-app.store-kv-changelog", Serdes.Long(), WrapperValueSerde.create(Audit.AuditSerde(), Serdes.String()));

    private static class RecordSaver implements ExtendedProcessor<Long, String, Long, String> {

        private WrappedKeyValueStore<Long, String, Audit> store;

        @Override
        public void init(ExtendedProcessorContext<Long, String, Long, String> context) {
            store = context.getStateStore("kv");
        }

        @Override
        public void process(Record<Long, String> record) {
            if (record.value() == null) {
                store.delete(record.key());
            } else {
                store.putValue(record.key(), record.value());
            }
        }

    }

    @Configuration
    public static class TopologyConfig {
        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                WrappedKeyValueStoreBuilder<Long, String, Audit, ExtendedProcessorContext> store = Stores2.wrapKeyValueStoreBuilder(Stores.persistentKeyValueStore("kv"), Serdes.Long(), Serdes.String(), Audit.AuditSerde(), AuditWrapperSupplier::new);

                builder
                        .addProcessor(RecordSaver::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .done();
            };

        }

    }

    @Test
    void shouldAuditChanges() {
        send(SOURCE, 1L, "a");
        ConsumerRecord<Long, WrapperValue<Audit, String>> changelog = waitForRecordFrom(CHANGELOG);
        long firstCreatedAt = changelog.value().wrapper().createdAt();
        long firstModifiedAt = changelog.value().wrapper().modifiedAt();

        assertEquals(1L, changelog.key());
        assertEquals("a", changelog.value().value());
        assertEquals(1, changelog.value().wrapper().version());
        assertEquals(0, changelog.value().wrapper().partition());
        assertTrue(firstCreatedAt > 0);
        assertTrue(firstModifiedAt > 0);

        send(SOURCE, 2L, "b");
        changelog = waitForRecordFrom(CHANGELOG);
        assertEquals(2L, changelog.key());
        assertEquals("b", changelog.value().value());
        assertEquals(1, changelog.value().wrapper().version());
        assertEquals(0, changelog.value().wrapper().partition());

        send(SOURCE, 1L, "aa");
        changelog = waitForRecordFrom(CHANGELOG);
        assertEquals(1L, changelog.key());
        assertEquals("aa", changelog.value().value());
        assertEquals(2, changelog.value().wrapper().version());
        assertEquals(0, changelog.value().wrapper().partition());
        assertEquals(firstCreatedAt, changelog.value().wrapper().createdAt());
        assertNotEquals(firstModifiedAt, changelog.value().wrapper().modifiedAt());

        send(SOURCE, 2L, "bb");
        changelog = waitForRecordFrom(CHANGELOG);
        assertEquals(2L, changelog.key());
        assertEquals("bb", changelog.value().value());
        assertEquals(2, changelog.value().wrapper().version());
        assertEquals(0, changelog.value().wrapper().partition());
        assertEquals(false, changelog.value().wrapper().removed());

        send(SOURCE, 2L, null);
        changelog = waitForRecordFrom(CHANGELOG);
        assertEquals(2L, changelog.key());
        assertEquals("bb", changelog.value().value());
        assertEquals(3, changelog.value().wrapper().version());
        assertEquals(0, changelog.value().wrapper().partition());
        assertEquals(true, changelog.value().wrapper().removed());

        changelog = waitForRecordFrom(CHANGELOG);
        assertEquals(2L, changelog.key());
        assertEquals(null, changelog.value());
    }

    @Test
    void shouldParseHeaders() {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(SOURCE.topic, null, SOURCE.keySerde.serializer().serialize(null, 10L), SOURCE.valueSerde.serializer().serialize(null, "abc"));
        producerRecord.headers()
                .add(AuditWrapperSupplier.AuditHeaders.TRACE, "xxxx-123-xxx".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.USER, "aaa".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.REASON, "bbb".getBytes(StandardCharsets.UTF_8));
        send(producerRecord);

        ConsumerRecord<Long, WrapperValue<Audit, String>> changelog = waitForRecordFrom(CHANGELOG);

        assertEquals(10L, changelog.key());
        assertEquals("abc", changelog.value().value());
        assertEquals(1, changelog.value().wrapper().version());
        assertEquals(123, changelog.value().wrapper().traceId());
        assertEquals("aaa", changelog.value().wrapper().modifiedBy());
        assertEquals("bbb", changelog.value().wrapper().reason());
    }
}
