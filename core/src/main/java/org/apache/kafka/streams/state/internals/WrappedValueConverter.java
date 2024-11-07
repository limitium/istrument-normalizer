package org.apache.kafka.streams.state.internals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public abstract class WrappedValueConverter<W, V> implements Converter {
    WrappedConverter<W> wrapperConverter;
    WrappedConverter<V> valueConverter;
    WrapperValueSerde<W, V> wrapperValueSerde;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        wrapperConverter = getWrappedConverter();
        valueConverter = getValueConverter();
        wrapperValueSerde = WrapperValueSerde.create(wrapperConverter.getSerde(), valueConverter.getSerde());
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Struct struct) {
            WrappedValue<W, V> wrappedValue = new WrappedValue<>(wrapperConverter.createObject(schema, struct), valueConverter.createObject(schema, struct));
            return wrapperValueSerde.serializer().serialize(topic, wrappedValue);
        }
        return null;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        SchemaBuilder builder = SchemaBuilder.struct().name(getClass().getCanonicalName());
        Schema schema = wrapperConverter.fillSchema(valueConverter.fillSchema(builder)).build();

        Struct struct = null;
        if (value != null) {
            struct = new Struct(schema);
            WrappedValue<W, V> wrappedValue = wrapperValueSerde.deserializer().deserialize(topic, value);
            valueConverter.fillStruct(struct, wrappedValue.value());
            wrapperConverter.fillStruct(struct, wrappedValue.wrapper());
        }
        return new SchemaAndValue(schema, struct);
    }

    protected abstract WrappedConverter<W> getWrappedConverter();

    protected abstract WrappedConverter<V> getValueConverter();
}
