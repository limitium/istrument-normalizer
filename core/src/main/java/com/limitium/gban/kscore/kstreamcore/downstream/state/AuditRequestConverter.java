package com.limitium.gban.kscore.kstreamcore.downstream.state;

import com.limitium.gban.kscore.kstreamcore.audit.AuditValueConverter;
import org.apache.kafka.streams.state.internals.WrappedConverter;

public class AuditRequestConverter extends AuditValueConverter<Request> {

    @Override
    protected WrappedConverter<Request> getValueConverter() {
        return new RequestConverter();
    }
}
