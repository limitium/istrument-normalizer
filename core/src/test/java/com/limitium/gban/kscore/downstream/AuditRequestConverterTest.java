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
import org.apache.kafka.streams.state.internals.WrapperValue;
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
        WrapperValue<Audit, Request> wrapperValue = new WrapperValue<>(
                new Audit(1, 2, 3, 4L, 5L, null, null, false),
                new Request(1L, "qwe", Request.RequestType.NEW, 2L, 3, 4L, 5, 6, 123L));
        SchemaAndValue schemaAndValue = auditStringConverter.toConnectData(null, wrapperValueSerde.serializer().serialize(null, wrapperValue));

        byte[] bytes = auditStringConverter.fromConnectData(null, schemaAndValue.schema(), schemaAndValue.value());

        WrapperValue<Audit, Request> deserialized = wrapperValueSerde.deserializer().deserialize(null, bytes);

        assertEquals(wrapperValue.value().id, deserialized.value().id);
        assertEquals(wrapperValue.value().correlationId, deserialized.value().correlationId);
        assertEquals(wrapperValue.value().type, deserialized.value().type);
        assertEquals(wrapperValue.value().effectiveReferenceId, deserialized.value().effectiveReferenceId);
        assertEquals(wrapperValue.value().effectiveVersion, deserialized.value().effectiveVersion);
        assertEquals(wrapperValue.value().referenceId, deserialized.value().referenceId);
        assertEquals(wrapperValue.value().referenceVersion, deserialized.value().referenceVersion);
        assertEquals(wrapperValue.value().overrideVersion, deserialized.value().overrideVersion);
        assertEquals(wrapperValue.wrapper().traceId(), deserialized.wrapper().traceId());
        assertEquals(wrapperValue.wrapper().version(), deserialized.wrapper().version());
        assertEquals(wrapperValue.wrapper().createdAt(), deserialized.wrapper().createdAt());
        assertEquals(wrapperValue.wrapper().modifiedAt(), deserialized.wrapper().modifiedAt());
        assertEquals(wrapperValue.wrapper().modifiedBy(), deserialized.wrapper().modifiedBy());
        assertEquals(wrapperValue.wrapper().reason(), deserialized.wrapper().reason());
        assertEquals(wrapperValue.wrapper().removed(), deserialized.wrapper().removed());
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
