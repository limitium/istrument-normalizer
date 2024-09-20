package com.limitium.gban.kscore.kstreamcore.dlq;

import com.limitium.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Serde;

public class PojoEnvelopedDLQ<KIn, VIn> extends EnvelopedDLQ<KIn, VIn> {
    public PojoEnvelopedDLQ(Serde<VIn> sourceValueSerde, DLQTopic<KIn> dlqTopic) {
        super(dlqTopic, new PojoDLQTransformer<>(sourceValueSerde));
    }

    public PojoEnvelopedDLQ(Topic<KIn, VIn> sourceTopic, DLQTopic<KIn> dlqTopic) {
        this(sourceTopic.valueSerde, dlqTopic);
    }

    public PojoEnvelopedDLQ(Topic<KIn, VIn> sourceTopic, String dlqTopic) {
        this(sourceTopic.valueSerde, DLQTopic.createFor(sourceTopic, dlqTopic));
    }
}
