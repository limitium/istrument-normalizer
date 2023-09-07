package com.limitium.gban.usstreetprocessor.common.messages;

public class BookingRequest {
    public String correlationId;
    long allocationId;

    public BookingRequest(String correlationId, TigerAllocation tigerAllocation) {
        this.correlationId = correlationId;
        this.allocationId = tigerAllocation.id;
    }
}
