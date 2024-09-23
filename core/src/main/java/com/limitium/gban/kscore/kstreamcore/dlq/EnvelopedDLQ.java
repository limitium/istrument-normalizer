package com.limitium.gban.kscore.kstreamcore.dlq;

import org.apache.kafka.streams.state.internals.WrapperValue;

public class EnvelopedDLQ<KIn, VIn> extends DLQ<KIn, VIn, WrapperValue<DLQEnvelope, VIn>> {
    public EnvelopedDLQ(DLQTopic<KIn, VIn> dlqTopic, DLQTransformer<KIn, VIn, WrapperValue<DLQEnvelope, VIn>> transformer) {
        super(dlqTopic, transformer);
    }
}
