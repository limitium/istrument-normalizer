package com.limitium.gban.kscore.kstreamcore.downstream.state;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Request {
    public long id;
    public String correlationId;
    public Request.RequestType type;
    public Request.RequestState state;
    public long effectiveReferenceId;
    public int effectiveVersion;
    public long referenceId;
    public int referenceVersion;
    public int overrideVersion;
    public long createdAt;
    public long respondedAt;
    public String respondedCode;
    public String respondedMessage;
    public String externalId;
    public int externalVersion;

    public Request(long id, String correlationId, Request.RequestType requestType, long effectiveReferenceId, int effectiveVersion, long referenceId, int referenceVersion, int overrideVersion, long createdAt) {
        this.id = id;
        this.correlationId = correlationId;
        this.type = requestType;
        this.effectiveReferenceId = effectiveReferenceId;
        this.effectiveVersion = effectiveVersion;
        this.referenceId = referenceId;
        this.referenceVersion = referenceVersion;
        this.overrideVersion = overrideVersion;
        this.createdAt = createdAt;
        this.state = Request.RequestState.PENDING;
    }

    public Request(long id, String correlationId, Request.RequestType type, Request.RequestState state, long effectiveReferenceId, int effectiveVersion, long referenceId, int referenceVersion, int overrideVersion, long createdAt, long respondedAt, String respondedCode, String respondedMessage, String externalId, int externalVersion) {
        this.id = id;
        this.correlationId = correlationId;
        this.type = type;
        this.state = state;
        this.effectiveReferenceId = effectiveReferenceId;
        this.effectiveVersion = effectiveVersion;
        this.referenceId = referenceId;
        this.referenceVersion = referenceVersion;
        this.overrideVersion = overrideVersion;
        this.createdAt = createdAt;
        this.respondedAt = respondedAt;
        this.respondedCode = respondedCode;
        this.respondedMessage = respondedMessage;
        this.externalId = externalId;
        this.externalVersion = externalVersion;
    }

    public String getStoreKey() {
        return referenceId + "_" + id;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    @Override
    public String toString() {
        return "Request{" +
                "id=" + id +
                ", correlationId='" + correlationId + '\'' +
                ", type=" + type +
                ", state=" + state +
                ", effectiveReferenceId=" + effectiveReferenceId +
                ", effectiveVersion=" + effectiveVersion +
                ", referenceId=" + referenceId +
                ", referenceVersion=" + referenceVersion +
                ", overrideVersion=" + overrideVersion +
                ", createdAt=" + createdAt +
                ", respondedAt=" + respondedAt +
                ", respondedCode='" + respondedCode + '\'' +
                ", respondedMessage='" + respondedMessage + '\'' +
                ", externalId='" + externalId + '\'' +
                ", externalVersion=" + externalVersion +
                '}';
    }

    public enum RequestState {
        PENDING, ACKED, NACKED, TERMINATED
    }

    public enum RequestType {
        NEW, AMEND, CANCEL, SKIP
    }

    public static RequestSerde RequestSerde() {
        return new RequestSerde();
    }

    public static class RequestSerde implements Serde<Request> {

        @Override
        public Serializer<Request> serializer() {
            return new Serializer<Request>() {
                @Override
                public byte[] serialize(String topic, Request message) {
                    int fixFieldsLength = 5 * 8 + 6 * 4 + 4 * 4;
                    byte[] correlationIdBytes = message.correlationId != null ? message.correlationId.getBytes(StandardCharsets.UTF_8) : new byte[0];
                    byte[] respondedCodeBytes = message.respondedCode != null ? message.respondedCode.getBytes(StandardCharsets.UTF_8) : new byte[0];
                    byte[] respondedMessageBytes = message.respondedMessage != null ? message.respondedMessage.getBytes(StandardCharsets.UTF_8) : new byte[0];
                    byte[] externalIdBytes = message.externalId != null ? message.externalId.getBytes(StandardCharsets.UTF_8) : new byte[0];

                    int varyFieldsLength = correlationIdBytes.length + respondedCodeBytes.length + respondedMessageBytes.length + externalIdBytes.length;

                    ByteBuffer bb = ByteBuffer.allocate(fixFieldsLength + varyFieldsLength);
                    bb.putLong(message.id);
                    bb.putLong(message.effectiveReferenceId);
                    bb.putLong(message.referenceId);
                    bb.putLong(message.createdAt);
                    bb.putLong(message.respondedAt);
                    bb.putInt(message.effectiveVersion);
                    bb.putInt(message.referenceVersion);
                    bb.putInt(message.overrideVersion);
                    bb.putInt(message.externalVersion);

                    bb.putInt(message.state.ordinal());
                    bb.putInt(message.type.ordinal());

                    bb.putInt(correlationIdBytes.length);
                    bb.put(correlationIdBytes);
                    bb.putInt(respondedCodeBytes.length);
                    bb.put(respondedCodeBytes);
                    bb.putInt(respondedMessageBytes.length);
                    bb.put(respondedMessageBytes);
                    bb.putInt(externalIdBytes.length);
                    bb.put(externalIdBytes);

                    return bb.array();
                }
            };
        }

        @Override
        public Deserializer<Request> deserializer() {
            return new Deserializer<>() {
                @Override
                public Request deserialize(String topic, byte[] message) {
                    ByteBuffer bb = ByteBuffer.wrap(message);

                    long id = bb.getLong();
                    long effectiveReferenceId = bb.getLong();
                    long referenceId = bb.getLong();
                    long createdAt = bb.getLong();
                    long respondedAt = bb.getLong();

                    int effectiveVersion = bb.getInt();
                    int referenceVersion = bb.getInt();
                    int overrideVersion = bb.getInt();
                    int externalVersion = bb.getInt();

                    RequestState requestState = RequestState.values()[bb.getInt()];
                    RequestType requestType = RequestType.values()[bb.getInt()];

                    String correlationId = getString(bb);
                    String respondedCode = getString(bb);
                    String respondedMessage = getString(bb);
                    String externalId = getString(bb);

                    return new Request(
                            id,
                            correlationId,
                            requestType,
                            requestState,
                            effectiveReferenceId,
                            effectiveVersion,
                            referenceId,
                            referenceVersion,
                            overrideVersion,
                            createdAt,
                            respondedAt,
                            respondedCode,
                            respondedMessage,
                            externalId,
                            externalVersion
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
