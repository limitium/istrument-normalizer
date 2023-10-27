package com.limitium.gban.kscore.kstreamcore.audit;

import org.apache.kafka.streams.state.internals.WrappedConverter;
import org.apache.kafka.streams.state.internals.WrappedValueConverter;

public abstract class AuditValueConverter<V> extends WrappedValueConverter<Audit, V> {
    @Override
    protected WrappedConverter<Audit> getWrappedConverter() {
        return new AuditConverter();
    }
}
