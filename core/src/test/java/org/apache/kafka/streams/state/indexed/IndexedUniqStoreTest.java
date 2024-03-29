package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexedUniqStoreTest {

    protected InternalMockProcessorContext<Integer, String> context;
    protected IndexedMeteredKeyValueStore<Integer, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @BeforeEach
    public void setUp() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext<Integer, String>) driver.context();
        context.setTime(10);

        store = createStore(context);
    }

    private IndexedMeteredKeyValueStore<Integer, String> createStore(InternalMockProcessorContext<Integer, String> context) {
        IndexedKeyValueStoreBuilder<Integer, String> builder = Stores2.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("my-store"),
                        Serdes.Integer(),
                        Serdes.String())
                //Build uniq index based on first char
                .addUniqIndex("idx", (v) -> String.valueOf(v.charAt(0)));

        IndexedMeteredKeyValueStore<Integer, String> store = builder.build();

        store.init((StateStoreContext) context, store);

        store.onPostInit(getProcessorContext(store));
        return store;
    }


    ProcessorContext getProcessorContext(IndexedMeteredKeyValueStore<Integer, String> store) {
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
    void shouldReturnIndexedValue() {
        store.put(1, "aa");
        store.put(2, "bb");
        store.put(3, "cc");

        assertEquals("aa", store.getUnique("idx", "a"));
        assertEquals("bb", store.getUnique("idx", "b"));
        assertEquals("cc", store.getUnique("idx", "c"));
    }

    @Test
    void shouldThrowUniqKeyViolationForTheSameIndexKey() {
        store.put(1, "aa");
        assertThrows(UniqKeyViolationException.class, () -> store.put(2, "ab"));
    }

    @Test
    void shouldRemoveValueFromIndexOnDelete() {
        store.put(1, "aa");
        assertEquals("aa", store.getUnique("idx", "a"));

        store.delete(1);
        assertNull(store.getUnique("idx", "a"));
    }

    @Test
    void shouldThrowRuntimeExceptionOnNonImplementedMethods() {
        assertThrows(RuntimeException.class, () -> store.putAll(null));
        assertThrows(RuntimeException.class, () -> store.putIfAbsent(null, null));
    }

    @Test
    void shouldRebuildIndexOnRestore() {
        store.close();

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "aa");
        driver.addEntryToRestoreLog(1, "bb");
        driver.addEntryToRestoreLog(2, "cc");
        driver.addEntryToRestoreLog(2, null);

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createStore((InternalMockProcessorContext<Integer, String>) driver.context());
        context.restore(store.name(), driver.restoredEntries());
        store.onPostInit(getProcessorContext(store));

        // Verify that the store's changelog does not get more appends ...
        assertEquals(0, driver.numFlushedEntryStored());
        assertEquals(0, driver.numFlushedEntryRemoved());

        // and there are no other entries ...
        assertEquals(2, driver.sizeOf(store));

        assertEquals("aa", store.getUnique("idx", "a"));
        assertEquals("bb", store.getUnique("idx", "b"));
        assertNull(store.getUnique("idx", "c"));
    }
}
