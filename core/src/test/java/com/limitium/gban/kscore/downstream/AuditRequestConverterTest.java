package com.limitium.gban.kscore.downstream;

import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.audit.AuditValueConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.state.AuditRequestConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.state.internals.WrappedConverter;
import org.apache.kafka.streams.state.internals.WrappedValue;
import org.apache.kafka.streams.state.internals.WrapperValueSerde;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuditRequestConverterTest {

    @Test
    void testConverter() {
        AuditRequestConverter auditStringConverter = new AuditRequestConverter();
        auditStringConverter.configure(null, false);

        WrapperValueSerde<Audit, Request> wrapperValueSerde = WrapperValueSerde.create(Audit.AuditSerde(), Request.RequestSerde());
        WrappedValue<Audit, Request> wrappedValue = new WrappedValue<>(
                new Audit(1, 2, 3, 4L, 5L, null, null, false),
                new Request(1L, "qwe", Request.RequestType.NEW, 2L, 3, 4L, 5, 6, 123L));
        SchemaAndValue schemaAndValue = auditStringConverter.toConnectData(null, wrapperValueSerde.serializer().serialize(null, wrappedValue));

        byte[] bytes = auditStringConverter.fromConnectData(null, schemaAndValue.schema(), schemaAndValue.value());

        WrappedValue<Audit, Request> deserialized = wrapperValueSerde.deserializer().deserialize(null, bytes);

        assertEquals(wrappedValue.value().id, deserialized.value().id);
        assertEquals(wrappedValue.value().correlationId, deserialized.value().correlationId);
        assertEquals(wrappedValue.value().type, deserialized.value().type);
        assertEquals(wrappedValue.value().effectiveReferenceId, deserialized.value().effectiveReferenceId);
        assertEquals(wrappedValue.value().effectiveVersion, deserialized.value().effectiveVersion);
        assertEquals(wrappedValue.value().referenceId, deserialized.value().referenceId);
        assertEquals(wrappedValue.value().referenceVersion, deserialized.value().referenceVersion);
        assertEquals(wrappedValue.value().overrideVersion, deserialized.value().overrideVersion);
        assertEquals(wrappedValue.wrapper().traceId(), deserialized.wrapper().traceId());
        assertEquals(wrappedValue.wrapper().version(), deserialized.wrapper().version());
        assertEquals(wrappedValue.wrapper().createdAt(), deserialized.wrapper().createdAt());
        assertEquals(wrappedValue.wrapper().modifiedAt(), deserialized.wrapper().modifiedAt());
        assertEquals(wrappedValue.wrapper().modifiedBy(), deserialized.wrapper().modifiedBy());
        assertEquals(wrappedValue.wrapper().reason(), deserialized.wrapper().reason());
        assertEquals(wrappedValue.wrapper().removed(), deserialized.wrapper().removed());
    }

    @NotNull
    private AuditValueConverter<String> getStringAuditValueConverter() {
        AuditValueConverter<String> auditStringConverter = new AuditValueConverter<>() {
            @Override
            protected WrappedConverter<String> getValueConverter() {
                return new WrappedConverter<>() {
                    @Override
                    public SchemaBuilder fillSchema(SchemaBuilder builder) {
                        return builder.field("STR_FIELD", Schema.STRING_SCHEMA);
                    }

                    @Override
                    public void fillStruct(Struct struct, String obj) {
                        struct.put("STR_FIELD", obj);
                    }

                    @Override
                    public String createObject(Schema schema, Struct struct) {
                        return struct.getString("STR_FIELD");
                    }

                    @Override
                    public Serde<String> getSerde() {
                        return Serdes.String();
                    }
                };
            }
        };

        auditStringConverter.configure(null, false);
        return auditStringConverter;
    }
}
