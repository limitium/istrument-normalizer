package com.limitium.gban.kscore.audit;

import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.audit.AuditValueConverter;
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

public class AuditValueConverterTest {

    @Test
    void testConverter() {
        AuditValueConverter<String> auditStringConverter = getStringAuditValueConverter();

        WrapperValueSerde<Audit, String> wrapperValueSerde = WrapperValueSerde.create(Audit.AuditSerde(), Serdes.String());
        WrapperValue<Audit, String> wrapperValue = new WrapperValue<>(new Audit(1, 2, 3, 4L, 5L, null, null, false), "abc123");
        SchemaAndValue schemaAndValue = auditStringConverter.toConnectData(null, wrapperValueSerde.serializer().serialize(null, wrapperValue));

        byte[] bytes = auditStringConverter.fromConnectData(null, schemaAndValue.schema(), schemaAndValue.value());

        WrapperValue<Audit, String> deserialized = wrapperValueSerde.deserializer().deserialize(null, bytes);

        assertEquals(wrapperValue.value(), deserialized.value());
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
