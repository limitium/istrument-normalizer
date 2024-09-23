package com.limitium.gban.kscore.dlq;

import com.limitium.gban.kscore.kstreamcore.dlq.DLQEnvelope;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DLQEnvelopeSerdeTest {

    @Test
    void testSerde() {
        DLQEnvelope.DLQEnvelopeSerde dlqEnvelopeSerde = new DLQEnvelope.DLQEnvelopeSerde();

        DLQEnvelope dlqEnvelope = new DLQEnvelope(1, "hi", "1/2/3", "Class123", "[1,2,3]", "topic1", 1, 1, "excpCls1", "key1", "subkey1", 123);
        byte[] bytes = dlqEnvelopeSerde.serializer().serialize(null, dlqEnvelope);
        DLQEnvelope deserialized = dlqEnvelopeSerde.deserializer().deserialize(null, bytes);

        assertEquals(dlqEnvelope.id(), deserialized.id());
        assertEquals(dlqEnvelope.message(), deserialized.message());
        assertEquals(dlqEnvelope.stacktrace(), deserialized.stacktrace());
        assertEquals(dlqEnvelope.payloadClass(), deserialized.payloadClass());
        assertEquals(dlqEnvelope.payloadBodyJSON(), deserialized.payloadBodyJSON());
        assertEquals(dlqEnvelope.sourceTopic(), deserialized.sourceTopic());
        assertEquals(dlqEnvelope.sourcePartition(), deserialized.sourcePartition());
        assertEquals(dlqEnvelope.sourceOffset(), deserialized.sourceOffset());
        assertEquals(dlqEnvelope.exceptionClass(), deserialized.exceptionClass());
        assertEquals(dlqEnvelope.exceptionKey(), deserialized.exceptionKey());
        assertEquals(dlqEnvelope.exceptionSubKey(), deserialized.exceptionSubKey());
        assertEquals(dlqEnvelope.failedAt(), deserialized.failedAt());

    }

    @Test
    void testSerdeWithNulls() {
        DLQEnvelope.DLQEnvelopeSerde dlqEnvelopeSerde = new DLQEnvelope.DLQEnvelopeSerde();

        DLQEnvelope dlqEnvelope = new DLQEnvelope(-1, null, null, null, null, null, -1, -1, null, null, null, -1);
        byte[] bytes = dlqEnvelopeSerde.serializer().serialize(null, dlqEnvelope);
        DLQEnvelope deserialized = dlqEnvelopeSerde.deserializer().deserialize(null, bytes);

        assertEquals(dlqEnvelope.id(), deserialized.id());
        assertEquals(dlqEnvelope.message(), deserialized.message());
        assertEquals(dlqEnvelope.stacktrace(), deserialized.stacktrace());
        assertEquals(dlqEnvelope.payloadClass(), deserialized.payloadClass());
        assertEquals(dlqEnvelope.payloadBodyJSON(), deserialized.payloadBodyJSON());
        assertEquals(dlqEnvelope.sourceTopic(), deserialized.sourceTopic());
        assertEquals(dlqEnvelope.sourcePartition(), deserialized.sourcePartition());
        assertEquals(dlqEnvelope.sourceOffset(), deserialized.sourceOffset());
        assertEquals(dlqEnvelope.exceptionClass(), deserialized.exceptionClass());
        assertEquals(dlqEnvelope.exceptionKey(), deserialized.exceptionKey());
        assertEquals(dlqEnvelope.exceptionSubKey(), deserialized.exceptionSubKey());
        assertEquals(dlqEnvelope.failedAt(), deserialized.failedAt());
    }
}
