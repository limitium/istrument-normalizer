package com.limitium.gban.kscore.kstreamcore.dlq;

import org.apache.kafka.streams.state.internals.WrappedConverter;
import org.apache.kafka.streams.state.internals.WrappedValueConverter;

public abstract class DLQEnvelopeMessageConverter<M> extends WrappedValueConverter<DLQEnvelope, M> {
    @Override
    protected WrappedConverter<DLQEnvelope> getWrappedConverter() {
        return new DLQEnvelopeWrappedConverter();
    }
}
