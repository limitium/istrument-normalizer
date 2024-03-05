package org.apache.kafka.streams.state.wrapped;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.ProcessorPostInitListener;
import org.apache.kafka.streams.state.internals.WrappedIndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WrapperSupplier;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WrappedIndexedStoreTest {

    protected InternalMockProcessorContext<Integer, String> context;
    protected WrappedIndexedKeyValueStore<Integer, String, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @BeforeEach
    public void setUp() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext<Integer, String>) driver.context();
        context.setTime(10);

        store = createStore(context);
    }

    private WrappedIndexedKeyValueStore<Integer, String, String> createStore(InternalMockProcessorContext<Integer, String> context) {
        WrappedIndexedKeyValueStoreBuilder<Integer, String, String, ProcessorContext> builder = Stores2.wrapIndexedKeyValueStoreBuilder(Stores.lruMap("my-store", 10), Serdes.Integer(), Serdes.String(), Serdes.String(), new WrapperSupplier.WrapperSupplierFactory<Integer, String, String, ProcessorContext>() {
            @Override
            public WrapperSupplier<Integer, String, String, ProcessorContext> create(WrappedKeyValueStore<Integer, String, String> store, ProcessorContext context) {
                return new WrapperSupplier<>(store, context) {
                    @Override
                    public String generate(Integer key, String value) {
                        return String.format("|%d,%s|", key * 2, value + "!");
                    }
                };
            }
        });

        builder.addUniqIndex("idx", (v) -> String.valueOf(v.value().charAt(0)));

        WrappedIndexedKeyValueStore<Integer, String, String> store = builder.build();

        store.init((StateStoreContext) context, store);
        if (store instanceof ProcessorPostInitListener wrappedStore) {
            wrappedStore.onPostInit(getProcessorContext(store));
        }
        return store;
    }
    ProcessorContext getProcessorContext(WrappedIndexedKeyValueStore<Integer, String, String> store) {
        ProcessorContext processorContext = mock(ProcessorContext.class);

        KeyValueStore<String, Integer> idxStore = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("my-store_idx"),
                        Serdes.String(),
                        Serdes.Integer())
                .build();

        idxStore.init(KeyValueStoreTestDriver.create(String.class, Integer.class).context(), store);
        when(processorContext.getStateStore(anyString()))
                .thenReturn(idxStore);
        return processorContext;
    }

    @AfterEach
    public void clean() {
        store.close();
        driver.clear();
    }


    @Test
    void shouldReturnWrappedIndexedValue() {
        store.putValue(1, "aa");
        store.putValue(2, "bb");
        store.putValue(3, "cc");

        assertEquals("|2,aa!|", store.getWrapper(1));
        assertEquals("aa", store.getValue(1));
        assertEquals("|4,bb!|", store.getWrapper(2));
        assertEquals("bb", store.getValue(2));
        assertEquals("|6,cc!|", store.getWrapper(3));
        assertEquals("cc", store.getValue(3));

        assertEquals("aa", store.getUnique("idx", "a").value());
        assertEquals("bb", store.getUnique("idx", "b").value());
        assertEquals("cc", store.getUnique("idx", "c").value());
    }
}
