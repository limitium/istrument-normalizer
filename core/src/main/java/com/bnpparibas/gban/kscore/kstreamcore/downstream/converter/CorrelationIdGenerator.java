package com.bnpparibas.gban.kscore.kstreamcore.downstream.converter;

public interface CorrelationIdGenerator<RequestData> {
    default String generate(long requestId, RequestData requestData) {
        return String.valueOf(requestId);
    }
}
