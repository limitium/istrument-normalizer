package com.limitium.gban.kscore.kstreamcore.dlq;

import com.limitium.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.internals.WrapperValue;
import org.apache.kafka.streams.state.internals.WrapperValueSerde;

public class DLQTopic<K, V> extends Topic<K, WrapperValue<DLQEnvelope, V>> {
    public DLQTopic(String dlqTopic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(dlqTopic, keySerde, new WrapperValueSerde<>(new DLQEnvelope.DLQEnvelopeSerde(), valueSerde));
    }

    public static <K, V> DLQTopic<K, V> createFor(Topic<K, V> topic, String dlqTopic) {
        return new DLQTopic<>(dlqTopic, topic.keySerde, topic.valueSerde);
    }
}
