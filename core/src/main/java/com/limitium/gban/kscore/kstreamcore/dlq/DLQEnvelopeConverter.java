package com.limitium.gban.kscore.kstreamcore.dlq;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class DLQEnvelopeConverter implements Converter {
    private final DLQEnvelopeWrappedConverter dlqEnvelopeWrappedConverter = new DLQEnvelopeWrappedConverter();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Struct struct) {
                return dlqEnvelopeWrappedConverter.getSerde().serializer().serialize(topic, dlqEnvelopeWrappedConverter.createObject(schema, struct));
            }
            return null;
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        SchemaBuilder builder = SchemaBuilder.struct().name(DLQEnvelope.class.getCanonicalName());
        Schema schema = fillSchema(builder).build();

        Struct struct = null;
        if (value != null) {
            struct = new Struct(schema);
            fillStruct(struct, dlqEnvelopeWrappedConverter.getSerde().deserializer().deserialize(topic, value));
        }
        return new SchemaAndValue(schema, struct);
    }


    public SchemaBuilder fillSchema(SchemaBuilder builder) {
        return dlqEnvelopeWrappedConverter.fillSchema(builder);
    }

    public void fillStruct(Struct struct, DLQEnvelope obj) {
        dlqEnvelopeWrappedConverter.fillStruct(struct, obj);
    }
}
