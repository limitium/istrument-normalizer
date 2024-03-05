package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.state.KeyValueStore;

public class KeyValueReadWriteDecorator<K, V> extends AbstractReadWriteDecorator.KeyValueStoreReadWriteDecorator<K, V> {
    public KeyValueReadWriteDecorator(KeyValueStore<K, V> inner) {
        super(inner);
    }
}
