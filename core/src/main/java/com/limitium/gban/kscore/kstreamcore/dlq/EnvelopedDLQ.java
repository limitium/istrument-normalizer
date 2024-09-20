package com.limitium.gban.kscore.kstreamcore.dlq;

public class EnvelopedDLQ<KIn, VIn> extends DLQ<KIn, VIn, DLQEnvelope> {
    public EnvelopedDLQ(DLQTopic<KIn> dlqTopic, DLQTransformer<KIn, VIn, DLQEnvelope> transformer) {
        super(dlqTopic, transformer);
    }
}
