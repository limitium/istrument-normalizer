package com.limitium.gban.kscore.dlq;

import com.limitium.gban.kscore.audit.AuditWrapperSupplierTest.TestableExtendedProcessorContext;
import com.limitium.gban.kscore.kstreamcore.dlq.DLQEnvelope;
import com.limitium.gban.kscore.kstreamcore.dlq.DLQException;
import com.limitium.gban.kscore.kstreamcore.dlq.PojoDLQTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.internals.WrappedValue;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class PojoDLQTransformerTest {

    @Test
    void testTransformer() {
        PojoDLQTransformer<Long, String> transformer = new PojoDLQTransformer<>();

        Record<Long, WrappedValue<DLQEnvelope, String>> transformed = transformer.transform(new Record<Long, String>(1L, "qwe", 1234), new TestableExtendedProcessorContext(new ProcessorContext() {
            @Override
            public void forward(Record record) {

            }

            @Override
            public void forward(Record record, String childName) {

            }

            @Override
            public String applicationId() {
                return null;
            }

            @Override
            public TaskId taskId() {
                return new TaskId(1, 1);
            }

            @Override
            public Optional<RecordMetadata> recordMetadata() {
                return Optional.empty();
            }

            @Override
            public Serde<?> keySerde() {
                return null;
            }

            @Override
            public Serde<?> valueSerde() {
                return null;
            }

            @Override
            public File stateDir() {
                return null;
            }

            @Override
            public StreamsMetrics metrics() {
                return null;
            }

            @Override
            public <S extends StateStore> S getStateStore(String name) {
                return null;
            }

            @Override
            public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
                return null;
            }

            @Override
            public void commit() {

            }

            @Override
            public Map<String, Object> appConfigs() {
                return new HashMap<>();
            }

            @Override
            public Map<String, Object> appConfigsWithPrefix(String prefix) {
                return null;
            }

            @Override
            public long currentSystemTimeMs() {
                return 0;
            }

            @Override
            public long currentStreamTimeMs() {
                return 0;
            }
        }, null), "ERROR!", new DLQException("errorm", "key", "subkey"));

        assertNotNull(transformed);
        assertEquals(1L, transformed.key());
        assertInstanceOf(DLQEnvelope.class, transformed.value().wrapper());
        DLQEnvelope envelope = (DLQEnvelope) transformed.value().wrapper();

        assertEquals("ERROR!", envelope.message());
        assertEquals("\"qwe\"", envelope.payloadBodyJSON());
        assertEquals("java.lang.String", envelope.payloadClass());
        assertEquals("key", envelope.exceptionKey());
        assertEquals("subkey", envelope.exceptionSubKey());

        assertEquals("qwe", transformed.value().value());
    }

}
