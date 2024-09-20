package com.limitium.gban.kscore.kstreamcore.dlq;

import com.limitium.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Serde;

public class DLQTopic<K> extends Topic<K, DLQEnvelope> {
    public DLQTopic(String topic, Serde<K> keySerde) {
        super(topic, keySerde, new DLQEnvelope.DLQEnvelopeSerde());
    }

    public static <K, V> DLQTopic<K> createFor(Topic<K, V> topic, String toTopic) {
        return new DLQTopic<>(toTopic, topic.keySerde);
    }
}
