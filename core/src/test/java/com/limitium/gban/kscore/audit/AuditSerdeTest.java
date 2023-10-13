package com.limitium.gban.kscore.audit;

import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuditSerdeTest {

    @Test
    void testSerde() {
        Audit.AuditSerde auditSerde = new Audit.AuditSerde();

        Audit audit = new Audit(1, 2, 3, 4, 5, "qwe", "asd", false);
        byte[] bytes = auditSerde.serializer().serialize(null, audit);
        Audit deserialized = auditSerde.deserializer().deserialize(null, bytes);

        assertEquals(audit.traceId(), deserialized.traceId());
        assertEquals(audit.version(), deserialized.version());
        assertEquals(audit.partition(), deserialized.partition());
        assertEquals(audit.createdAt(), deserialized.createdAt());
        assertEquals(audit.modifiedAt(), deserialized.modifiedAt());
        assertEquals(audit.modifiedBy(), deserialized.modifiedBy());
        assertEquals(audit.reason(), deserialized.reason());
        assertEquals(audit.removed(), deserialized.removed());

    }

    @Test
    void testSerdeWithNulls() {
        Audit.AuditSerde auditSerde = new Audit.AuditSerde();

        Audit audit = new Audit(-1, 1, -1, 4, 5, null, null, true);
        byte[] bytes = auditSerde.serializer().serialize(null, audit);
        Audit deserialized = auditSerde.deserializer().deserialize(null, bytes);

        assertEquals(audit.traceId(), deserialized.traceId());
        assertEquals(audit.version(), deserialized.version());
        assertEquals(audit.partition(), deserialized.partition());
        assertEquals(audit.createdAt(), deserialized.createdAt());
        assertEquals(audit.modifiedAt(), deserialized.modifiedAt());
        assertEquals(audit.modifiedBy(), deserialized.modifiedBy());
        assertEquals(audit.reason(), deserialized.reason());
        assertEquals(audit.removed(), deserialized.removed());
    }
}
