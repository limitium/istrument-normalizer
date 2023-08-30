package com.bnpparibas.gban.kscore.downstream;

import com.bnpparibas.gban.kscore.kstreamcore.downstream.state.Request;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestsAwareSerdeTest {
    @Test
    void testSerde() {
        Request.RequestSerde requestSerde = Request.RequestSerde();

        Request request = new Request(
                1, "2",
                Request.RequestType.AMEND,
                Request.RequestState.PENDING,
                1,
                2,
                3,
                4,
                5,
                123,
                345,
                "qwe",
                "asd",
                "zxc",
                5);

        byte[] bytes = requestSerde.serializer().serialize("topic1", request);
        Request requestDeser = requestSerde.deserializer().deserialize("topic1", bytes);

        assertEquals(request.id, requestDeser.id);
        assertEquals(request.correlationId, requestDeser.correlationId);
        assertEquals(request.type, requestDeser.type);
        assertEquals(request.state, requestDeser.state);
        assertEquals(request.effectiveReferenceId, requestDeser.effectiveReferenceId);
        assertEquals(request.effectiveVersion, requestDeser.effectiveVersion);
        assertEquals(request.referenceId, requestDeser.referenceId);
        assertEquals(request.referenceVersion, requestDeser.referenceVersion);
        assertEquals(request.overrideVersion, requestDeser.overrideVersion);
        assertEquals(request.createdAt, requestDeser.createdAt);
        assertEquals(request.respondedAt, requestDeser.respondedAt);
        assertEquals(request.respondedCode, requestDeser.respondedCode);
        assertEquals(request.respondedMessage, requestDeser.respondedMessage);
        assertEquals(request.externalId, requestDeser.externalId);
        assertEquals(request.externalVersion, requestDeser.externalVersion);
    }
}
