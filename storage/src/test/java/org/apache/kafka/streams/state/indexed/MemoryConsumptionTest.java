package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MemoryConsumptionTest {
    Logger logger = LoggerFactory.getLogger(MemoryConsumptionTest.class);

    @Test
    void inserts5kk() {
        KeyValueStoreTestDriver<Long, String> driver = KeyValueStoreTestDriver.create(Long.class, String.class);
        InternalMockProcessorContext<Long, String> context = (InternalMockProcessorContext<Long, String>) driver.context();
        context.setTime(10);
        IndexedKeyValueStoreBuilder<Long, String> builder = Stores2.keyValueStoreBuilder(new KeyValueBytesStoreSupplier() {
                    @Override
                    public String name() {
                        return "root-name";
                    }

                    @Override
                    public KeyValueStore<Bytes, byte[]> get() {
                        return new KeyValueStore<Bytes, byte[]>() {
                            @Override
                            public void put(Bytes key, byte[] value) {

                            }

                            @Override
                            public byte[] putIfAbsent(Bytes key, byte[] value) {
                                return new byte[0];
                            }

                            @Override
                            public void putAll(List<KeyValue<Bytes, byte[]>> entries) {

                            }

                            @Override
                            public byte[] delete(Bytes key) {
                                return new byte[0];
                            }

                            @Override
                            public String name() {
                                return "in-store";
                            }

                            @Override
                            public void init(org.apache.kafka.streams.processor.ProcessorContext context, StateStore root) {

                            }

                            @Override
                            public void init(StateStoreContext context, StateStore root) {

                            }

                            @Override
                            public void flush() {

                            }

                            @Override
                            public void close() {

                            }

                            @Override
                            public boolean persistent() {
                                return false;
                            }

                            @Override
                            public boolean isOpen() {
                                return false;
                            }

                            @Override
                            public byte[] get(Bytes key) {
                                return new byte[0];
                            }

                            @Override
                            public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
                                return null;
                            }

                            @Override
                            public KeyValueIterator<Bytes, byte[]> all() {
                                return new KeyValueIterator<Bytes, byte[]>() {
                                    @Override
                                    public void close() {

                                    }

                                    @Override
                                    public Bytes peekNextKey() {
                                        return null;
                                    }

                                    @Override
                                    public boolean hasNext() {
                                        return false;
                                    }

                                    @Override
                                    public KeyValue<Bytes, byte[]> next() {
                                        return null;
                                    }
                                };
                            }

                            @Override
                            public long approximateNumEntries() {
                                return 0;
                            }
                        };
                    }

                    @Override
                    public String metricsScope() {
                        return "metrics";
                    }
                }, Serdes.Long(), Serdes.String()).withCachingDisabled().withLoggingDisabled()
                .addUniqIndex("un", (v) -> v)
                .addNonUniqIndex("non", (v) -> String.valueOf(Long.parseLong(v) + Long.parseLong(v) % 2))
                ;

        IndexedMeteredKeyValueStore<Long, String> store = builder.build();

        store.init((StateStoreContext) context, store);
        store.onPostInit(getProcessorContext(store));

        long startSeq = System.nanoTime();
        long start = System.currentTimeMillis();
        for (long i = 0; i < 5_000_000; i++) {
            long v = startSeq++;
            store.put(v, String.valueOf(v));
            if (i % 100_000 == 0) {
                logger.info("Inserted: {}", i);
            }
        }
        float insertTime = (System.currentTimeMillis() - start) / 1000f;
        logger.info("done for: {}s", insertTime);

        System.gc();
        long memUsage = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
        logger.info("Mem: {}Mb", memUsage);

        assertTrue(insertTime < 25);
        assertTrue(memUsage < 2000);
    }

    ProcessorContext getProcessorContext(IndexedMeteredKeyValueStore<Long, String> store) {
        ProcessorContext processorContext = mock(ProcessorContext.class);

        KeyValueStore<String, Long> idxStore = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("my-store_idx"),
                        Serdes.String(),
                        Serdes.Long())
                .withCachingDisabled().withLoggingDisabled()
                .build();

        idxStore.init(KeyValueStoreTestDriver.create(String.class, Long.class).context(), store);
        when(processorContext.getStateStore(anyString()))
                .thenReturn(idxStore);
        return processorContext;
    }
}