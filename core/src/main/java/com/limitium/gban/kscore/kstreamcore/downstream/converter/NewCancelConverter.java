package com.limitium.gban.kscore.kstreamcore.downstream.converter;

import org.apache.kafka.streams.processor.api.Record;

public interface NewCancelConverter<RequestData, Kout, Vout> {
    Record<Kout, Vout> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, RequestData requestData);

    Record<Kout, Vout> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, RequestData requestData);
}
