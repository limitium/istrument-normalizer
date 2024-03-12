package com.limitium.gban.kscore.audit;


import com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import com.limitium.gban.kscore.kstreamcore.processor.ProcessorMeta;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuditWrapperSupplierTest {
    static class TestableExtendedProcessorContext extends ExtendedProcessorContext {

        public TestableExtendedProcessorContext(ProcessorContext context, ProcessorMeta processorMeta) {
            super(context, processorMeta);
        }

        @Override
        public void beginProcessing(Record incomingRecord) {
            super.beginProcessing(incomingRecord);
        }
    }

    @Test
    void traceIdParserTest() {
        assertEquals(123L, extractTrace(Serdes.String().serializer().serialize(null, "xxx-123-xxx")));
        assertEquals(-1L, extractTrace(Serdes.String().serializer().serialize(null, "123")));
        assertEquals(-1L, extractTrace(Serdes.String().serializer().serialize(null, "qwe")));
        assertEquals(-1L, extractTrace(Serdes.Long().serializer().serialize(null, 123L)));
        assertEquals(-1L, extractTrace(Serdes.Long().serializer().serialize(null, -1L)));
        assertEquals(-1L, extractTrace(Serdes.Long().serializer().serialize(null, null)));
        assertEquals(-1L, extractTrace(null));
        assertEquals(-1L, extractTrace(new byte[0]));
        assertEquals(-1L, extractTrace(new byte[]{1, 2, 3}));
    }

    private Long extractTrace(byte[] value) {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext(new ProcessorContext() {
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
        }, null);

        Record incomingRecord = new Record(null, null, 0L);
        incomingRecord.headers().add(AuditWrapperSupplier.AuditHeaders.TRACE, value);
        context.beginProcessing(incomingRecord);

        return AuditWrapperSupplier.extractTraceId(context);
    }
}