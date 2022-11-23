package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexedMeteredKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(IndexedMeteredKeyValueStore.class);


    IndexedMeteredKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                                final String metricsScope,
                                final Time time,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        super(inner, metricsScope, time, keySerde, valueSerde);
    }
}
