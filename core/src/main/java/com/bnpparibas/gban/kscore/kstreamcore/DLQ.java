package com.bnpparibas.gban.kscore.kstreamcore;

public class DLQ<KIn, VIn, DLQm> {
    public DLQ(Topic<KIn, DLQm> topic, KSDLQTransformer<KIn, VIn, DLQm> transformer) {
        this.topic = topic;
        this.transformer = transformer;
    }

    Topic<KIn, DLQm> topic;
    KSDLQTransformer<KIn, VIn, DLQm> transformer;
}
