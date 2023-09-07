package com.limitium.gban.flatbufferstooling.communication.converter;

import com.limitium.gban.flatbufferstooling.communication.FlatbuffersSerde;
import com.google.flatbuffers.Table;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects schema definition from generated children classes and fills {@link Struct}
 *
 * @param <T> - flatbuffers table
 */
public abstract class FbModelToStructConverter<T extends Table> {
    private static final Logger log = LoggerFactory.getLogger(FbModelToStructConverter.class);
    final Deserializer<T> deserializer = new FlatbuffersSerde<T>(getClazz()).deserializer();

    public Schema getSchema() {
        return fillSchema(SchemaBuilder.struct().name(getClazz().getCanonicalName())).build();
    }

    public SchemaAndValue convert(byte[] data) {
        Schema schema = getSchema();
        Struct struct = new Struct(schema);

        if (data != null) {
            fillStruct(struct, deserializer.deserialize(null, data));
        } else {
            struct = null;
        }

        return new SchemaAndValue(schema, struct);
    }

    protected void putValueToStruct(Struct struct, String field, Object value) {
        struct.put(field, value);
    }

    protected void putEnumValueToStruct(Struct struct, String field, String[] names, Object value) {
        byte index = (byte) value;
        if (index < 0 || index >= names.length) {
            log.warn("Invalid index of {}: {}, names length is: {}", field, index, names.length);
            putValueToStruct(struct, field, "UDEFINED, index: " + index);
        } else {
            putValueToStruct(struct, field, names[index]);
        }
    }

    public abstract SchemaBuilder fillSchema(SchemaBuilder struct);

    public abstract void fillStruct(Struct struct, T obj);

    protected abstract Class<T> getClazz();
}
