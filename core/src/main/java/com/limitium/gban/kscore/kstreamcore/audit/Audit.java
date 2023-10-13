package com.limitium.gban.kscore.kstreamcore.audit;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record Audit(
        long traceId,
        int version,
        int partition,
        long createdAt,
        long modifiedAt,
        String modifiedBy,
        String reason,
        boolean removed
) {

    public static Audit.AuditSerde AuditSerde() {
        return new Audit.AuditSerde();
    }

    public static class AuditSerde implements Serde<Audit> {

        @Override
        public Serializer<Audit> serializer() {
            return new Serializer<Audit>() {
                @Override
                public byte[] serialize(String topic, Audit audit) {
                    int fixFieldsLength = 3 * 8 + 4 * 2 + 4 * 2 + 1;
                    byte[] modifiedByBytes = audit.modifiedBy != null ? audit.modifiedBy.getBytes(StandardCharsets.UTF_8) : new byte[0];
                    byte[] reasonBytes = audit.reason != null ? audit.reason.getBytes(StandardCharsets.UTF_8) : new byte[0];

                    int varyFieldsLength = modifiedByBytes.length + reasonBytes.length;

                    ByteBuffer bb = ByteBuffer.allocate(fixFieldsLength + varyFieldsLength);

                    bb.putLong(audit.traceId);
                    bb.putInt(audit.version);
                    bb.putInt(audit.partition);
                    bb.putLong(audit.createdAt);
                    bb.putLong(audit.modifiedAt);

                    bb.putInt(modifiedByBytes.length);
                    bb.put(modifiedByBytes);
                    bb.putInt(reasonBytes.length);
                    bb.put(reasonBytes);

                    bb.put((byte) (audit.removed ? 1 : 0));

                    return bb.array();
                }
            };
        }

        @Override
        public Deserializer<Audit> deserializer() {
            return new Deserializer<>() {
                @Override
                public Audit deserialize(String topic, byte[] message) {
                    ByteBuffer bb = ByteBuffer.wrap(message);

                    long traceId = bb.getLong();
                    int version = bb.getInt();
                    int partition = bb.getInt();
                    long createdAt = bb.getLong();
                    long modifiedAt = bb.getLong();

                    String modifiedBy = getString(bb);
                    String reason = getString(bb);

                    boolean removed = bb.get() == 1;

                    return new Audit(
                            traceId,
                            version,
                            partition,
                            createdAt,
                            modifiedAt,
                            modifiedBy,
                            reason,
                            removed
                    );
                }

                private String getString(ByteBuffer bb) {
                    int strLen = bb.getInt();
                    if (strLen == 0) {
                        return null;
                    }
                    byte[] strBytes = new byte[strLen];
                    bb.get(strBytes, 0, strLen);
                    return new String(strBytes, StandardCharsets.UTF_8);
                }
            };
        }
    }
}