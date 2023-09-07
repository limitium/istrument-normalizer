package com.limitium.gban.kscore.kstreamcore;

import org.apache.kafka.common.serialization.Serde;

public class Topic<K, V> {
    public String topic;
    public Serde<K> keySerde;
    public Serde<V> valueSerde;

    public Topic(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }
}
