package com.limitium.gban.kscore.kstreamcore.dlq;

import org.apache.kafka.streams.state.internals.WrappedValue;

public class EnvelopedDLQ<KIn, VIn> extends DLQ<KIn, VIn, WrappedValue<DLQEnvelope, VIn>> {
    public EnvelopedDLQ(DLQTopic<KIn, VIn> dlqTopic, DLQTransformer<KIn, VIn, WrappedValue<DLQEnvelope, VIn>> transformer) {
        super(dlqTopic, transformer);
    }
}
