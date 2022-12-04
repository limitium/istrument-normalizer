package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.createMock;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IndexedStoreBuilderTest {
    private KeyValueBytesStoreSupplier supplier;

    @BeforeEach
    public void setUp() {
        supplier = createMock(KeyValueBytesStoreSupplier.class);
    }

    @Test
    void shouldThrowNullPointerIfIndexNameIsNull() {
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(supplier, Serdes.String(), Serdes.String()).addUniqIndex(null, (v) -> null));
    }

    @Test
    void shouldThrowNullPointerIfKeyGeneratorIsNull() {
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(supplier, Serdes.String(), Serdes.String()).addUniqIndex("index", null));
    }

}
