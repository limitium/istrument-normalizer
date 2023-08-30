package com.bnpparibas.gban.kscore.kstreamcore.downstream.converter;

import org.apache.kafka.streams.processor.api.Record;

public interface AmendConverter<RequestData, Kout, Vout> extends NewCancelConverter<RequestData, Kout, Vout> {
    Record<Kout, Vout> amendRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, RequestData requestData);
}
