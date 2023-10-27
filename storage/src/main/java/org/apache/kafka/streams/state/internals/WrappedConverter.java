package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public interface WrappedConverter<C> {
    SchemaBuilder fillSchema(SchemaBuilder builder);

    void fillStruct(Struct struct, C obj);

    C createObject(Schema schema, Struct struct);

    Serde<C> getSerde();
}
