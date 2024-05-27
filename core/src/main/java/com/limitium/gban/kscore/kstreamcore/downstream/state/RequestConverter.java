package com.limitium.gban.kscore.kstreamcore.downstream.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.state.internals.WrappedConverter;

public class RequestConverter implements WrappedConverter<Request> {
    @Override
    public SchemaBuilder fillSchema(SchemaBuilder builder) {
        return builder
                .field("ID", Schema.INT64_SCHEMA)
                .field("CORRELATION_ID", Schema.STRING_SCHEMA)
                .field("TYPE", Schema.STRING_SCHEMA)
                .field("STATE", Schema.STRING_SCHEMA)
                .field("EFFECTIVE_REFERENCE_ID", Schema.INT64_SCHEMA)
                .field("EFFECTIVE_VERSION", Schema.INT32_SCHEMA)
                .field("REFERENCE_ID", Schema.INT64_SCHEMA)
                .field("REFERENCE_VERSION", Schema.INT32_SCHEMA)
                .field("OVERRIDE_VERSION", Schema.INT32_SCHEMA)
                .field("CREATED_AT", Schema.INT64_SCHEMA)
                .field("RESPONDED_AT", Schema.OPTIONAL_INT64_SCHEMA)
                .field("RESPONDED_CODE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("RESPONDED_MESSAGE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EXTERNAL_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EXTERNAL_VERSION", Schema.OPTIONAL_INT32_SCHEMA)
                .field("STORE_KEY", Schema.STRING_SCHEMA)
                ;
    }

    @Override
    public void fillStruct(Struct struct, Request request) {
        struct
                .put("ID", request.id)
                .put("CORRELATION_ID", request.correlationId)
                .put("TYPE", request.type.name())
                .put("STATE", request.state.name())
                .put("EFFECTIVE_REFERENCE_ID", request.effectiveReferenceId)
                .put("EFFECTIVE_VERSION", request.effectiveVersion)
                .put("REFERENCE_ID", request.referenceId)
                .put("REFERENCE_VERSION", request.referenceVersion)
                .put("OVERRIDE_VERSION", request.overrideVersion)
                .put("CREATED_AT", request.createdAt)
                .put("RESPONDED_AT", request.respondedAt)
                .put("RESPONDED_CODE", request.respondedCode)
                .put("RESPONDED_MESSAGE", request.respondedMessage)
                .put("EXTERNAL_ID", request.externalId)
                .put("EXTERNAL_VERSION", request.externalVersion)
                .put("STORE_KEY", request.getStoreKey())
        ;
    }

    @Override
    public Request createObject(Schema schema, Struct struct) {
        return new Request(
                struct.getInt64("ID"),
                struct.getString("CORRELATION_ID"),
                Request.RequestType.valueOf(struct.getString("TYPE")),
                Request.RequestState.valueOf(struct.getString("STATE")),
                struct.getInt64("EFFECTIVE_REFERENCE_ID"),
                struct.getInt32("EFFECTIVE_VERSION"),
                struct.getInt64("REFERENCE_ID"),
                struct.getInt32("REFERENCE_VERSION"),
                struct.getInt32("OVERRIDE_VERSION"),
                struct.getInt64("CREATED_AT"),
                struct.getInt64("RESPONDED_AT"),
                struct.getString("RESPONDED_CODE"),
                struct.getString("RESPONDED_MESSAGE"),
                struct.getString("EXTERNAL_ID"),
                struct.getInt32("EXTERNAL_VERSION")
        );

    }

    @Override
    public Serde<Request> getSerde() {
        return Request.RequestSerde();
    }

}
