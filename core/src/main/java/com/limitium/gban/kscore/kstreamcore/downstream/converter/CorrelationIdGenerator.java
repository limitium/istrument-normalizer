package com.limitium.gban.kscore.kstreamcore.downstream.converter;

import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;

public interface CorrelationIdGenerator<RequestData> {
    default String generate(long requestId, Request.RequestType requestType, RequestData requestData) {
        return String.valueOf(requestId);
    }
}
