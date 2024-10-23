package com.limitium.gban.kscore.kstreamcore.dlq;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.state.internals.WrappedConverter;

import static com.limitium.gban.kscore.kstreamcore.primitive.PrimitiveNulls.primitiveFrom;

public class DLQEnvelopeWrappedConverter implements WrappedConverter<DLQEnvelope> {

    public static String prefix = "DLQENVELOPE__";

    @Override
    public SchemaBuilder fillSchema(SchemaBuilder builder) {
        return builder
                .field(prefix + "ID", Schema.INT64_SCHEMA)
                .field(prefix + "MESSAGE", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "STACKTRACE", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "PAYLOAD_CLASS", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "PAYLOAD_BODY_JSON", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "SOURCE_TOPIC", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "SOURCE_PARTITION", Schema.INT32_SCHEMA)
                .field(prefix + "SOURCE_OFFSET", Schema.INT64_SCHEMA)
                .field(prefix + "EXCEPTION_CLASS", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "EXCEPTION_KEY", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "EXCEPTION_SUB_KEY", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "FAILED_AT", Schema.INT64_SCHEMA)
                ;
    }

    @Override
    public void fillStruct(Struct struct, DLQEnvelope dlqEnvelope) {
        struct
                .put(prefix + "ID", dlqEnvelope.id())
                .put(prefix + "MESSAGE", dlqEnvelope.message())
                .put(prefix + "STACKTRACE", dlqEnvelope.stacktrace())
                .put(prefix + "PAYLOAD_CLASS", dlqEnvelope.payloadClass())
                .put(prefix + "PAYLOAD_BODY_JSON", dlqEnvelope.payloadBodyJSON())
                .put(prefix + "SOURCE_TOPIC", dlqEnvelope.sourceTopic())
                .put(prefix + "SOURCE_PARTITION", dlqEnvelope.sourcePartition())
                .put(prefix + "SOURCE_OFFSET", dlqEnvelope.sourceOffset())
                .put(prefix + "EXCEPTION_CLASS", dlqEnvelope.exceptionClass())
                .put(prefix + "EXCEPTION_KEY", dlqEnvelope.exceptionKey())
                .put(prefix + "EXCEPTION_SUB_KEY", dlqEnvelope.exceptionSubKey())
                .put(prefix + "FAILED_AT", dlqEnvelope.failedAt())
        ;
    }

    @Override
    public DLQEnvelope createObject(Schema schema, Struct struct) {
        return new DLQEnvelope(
                struct.getInt64(prefix + "ID"),
                struct.getString(prefix + "MESSAGE"),
                struct.getString(prefix + "STACKTRACE"),
                struct.getString(prefix + "PAYLOAD_CLASS"),
                struct.getString(prefix + "PAYLOAD_BODY_JSON"),
                struct.getString(prefix + "SOURCE_TOPIC"),
                primitiveFrom(struct.getInt32(prefix + "SOURCE_PARTITION")),
                primitiveFrom(struct.getInt64(prefix + "SOURCE_OFFSET")),
                struct.getString(prefix + "EXCEPTION_CLASS"),
                struct.getString(prefix + "EXCEPTION_KEY"),
                struct.getString(prefix + "EXCEPTION_SUB_KEY"),
                primitiveFrom(struct.getInt64(prefix + "FAILED_AT"))
        );
    }

    @Override
    public Serde<DLQEnvelope> getSerde() {
        return DLQEnvelope.DLQEnvelopeSerde();
    }
}
