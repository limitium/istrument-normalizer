package com.limitium.gban.kscore.downstream;

import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class MainScenariosTest extends BaseDSTest {

    @Test
    void testNewAmendCancelRequest() {
        //new
        send(SOURCE, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        //amend
        send(SOURCE, 1, 2L);

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>2|2", ds1outNew.payload());

        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>2|2", ds2outAmend.payload());
        //cancel
        send(SOURCE, 1, 0L);

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outCancel = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals(ds1outNew.refId(), ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        assertEquals("cancel", ds2outCancel.requestType());
        assertEquals("1", ds2outCancel.refId());
        assertEquals("2", ds2outCancel.refVer());
        assertEquals("ds2", ds2outCancel.dsId());
        assertEquals("-", ds2outCancel.payload());
    }

    @Test
    void testReply() {
        send(SOURCE, 0, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        ConsumerRecord<String, Request> request1 = waitForRecordFrom(REQUESTS1);
        ConsumerRecord<String, Request> request2 = waitForRecordFrom(REQUESTS2);//pending
        request2 = waitForRecordFrom(REQUESTS2);//autocommit
        ConsumerRecord<String, Request> request3 = waitForRecordFrom(REQUESTS3);

        long request1Id = request1.value().id;
        assertEquals(Request.RequestType.NEW, request1.value().type);
        assertEquals(Request.RequestState.PENDING, request1.value().state);
        assertEquals(0, request1.value().respondedAt);

        assertEquals(Request.RequestType.NEW, request2.value().type);
        assertEquals(Request.RequestState.ACKED, request2.value().state);

        long request3Id = request3.value().id;
        assertEquals(Request.RequestType.NEW, request3.value().type);
        assertEquals(Request.RequestState.PENDING, request3.value().state);
        assertEquals(0, request3.value().respondedAt);


        send(REPLY1, 0, ds1outNew.correlationId(), "true,-,-");
        send(REPLY3, 0, ds3outNew.correlationId(), "false,123,bad");

        request1 = waitForRecordFrom(REQUESTS1);
        request3 = waitForRecordFrom(REQUESTS3);

        assertEquals(Request.RequestState.ACKED, request1.value().state);
        assertEquals(request1Id, request1.value().id);
        assertNotEquals(0, request1.value().respondedAt);

        assertEquals(Request.RequestState.NACKED, request3.value().state);
        assertEquals(request3Id, request3.value().id);
        assertEquals("123", request3.value().respondedCode);
        assertEquals("bad", request3.value().respondedMessage);
        assertNotEquals(0, request3.value().respondedAt);
    }

    @Test
    void testResend() {
        send(SOURCE, 0, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        ConsumerRecord<String, Request> request1 = waitForRecordFrom(REQUESTS1);
        ConsumerRecord<String, Request> request2 = waitForRecordFrom(REQUESTS2);//pending
        request2 = waitForRecordFrom(REQUESTS2);//autocommit
        ConsumerRecord<String, Request> request3 = waitForRecordFrom(REQUESTS3);

        long request1Id = request1.value().id;
        assertEquals(Request.RequestType.NEW, request1.value().type);
        assertEquals(Request.RequestState.PENDING, request1.value().state);
        assertEquals(0, request1.value().respondedAt);

        assertEquals(Request.RequestType.NEW, request2.value().type);
        assertEquals(Request.RequestState.ACKED, request2.value().state);

        long request3Id = request3.value().id;
        assertEquals(Request.RequestType.NEW, request3.value().type);
        assertEquals(Request.RequestState.PENDING, request3.value().state);
        assertEquals(0, request3.value().respondedAt);


        send(REPLY1, 0, ds1outNew.correlationId(), "true,-,-");
        send(REPLY3, 0, ds3outNew.correlationId(), "false,123,bad");

        request1 = waitForRecordFrom(REQUESTS1);
        request3 = waitForRecordFrom(REQUESTS3);

        assertEquals(Request.RequestState.ACKED, request1.value().state);
        assertEquals(request1Id, request1.value().id);
        assertNotEquals(0, request1.value().respondedAt);

        assertEquals(Request.RequestState.NACKED, request3.value().state);
        assertEquals(request3Id, request3.value().id);
        assertEquals("123", request3.value().respondedCode);
        assertEquals("bad", request3.value().respondedMessage);
        assertNotEquals(0, request3.value().respondedAt);


        send(RESEND1, 0, request1.value().referenceId, "");
        send(RESEND2, 0, request1.value().referenceId, "");
        send(RESEND3, 0, request1.value().referenceId, "");

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>1|1", ds2outAmend.payload());

        Outgoing ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("new", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("1", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("rd3>1|1", ds3outAmend.payload());
    }

    @Test
    void testCancel() {
        send(SOURCE, 0, 1, 1L);

        ConsumerRecord<String, Request> request1 = waitForRecordFrom(REQUESTS1);
        ConsumerRecord<String, Request> request2 = waitForRecordFrom(REQUESTS2);//pending
        request2 = waitForRecordFrom(REQUESTS2);//autocommit
        ConsumerRecord<String, Request> request3 = waitForRecordFrom(REQUESTS3);

        assertEquals(Request.RequestType.NEW, request1.value().type);
        assertEquals(Request.RequestState.PENDING, request1.value().state);
        assertEquals(0, request1.value().respondedAt);

        assertEquals(Request.RequestType.NEW, request2.value().type);
        assertEquals(Request.RequestState.ACKED, request2.value().state);

        assertEquals(Request.RequestType.NEW, request3.value().type);
        assertEquals(Request.RequestState.PENDING, request3.value().state);
        assertEquals(0, request3.value().respondedAt);

        send(CANCEL1, request1.value().referenceId, request1.value().id);
        send(CANCEL2, request2.value().referenceId, request2.value().id);
        send(CANCEL3, request3.value().referenceId, request3.value().id);

        request1 = waitForRecordFrom(REQUESTS1);
        request2 = waitForRecordFrom(REQUESTS2);
        request3 = waitForRecordFrom(REQUESTS3);

        assertEquals(Request.RequestType.NEW, request1.value().type);
        assertEquals(Request.RequestState.CANCELED, request1.value().state);

        assertEquals(Request.RequestType.NEW, request2.value().type);
        assertEquals(Request.RequestState.CANCELED, request2.value().state);

        assertEquals(Request.RequestType.NEW, request3.value().type);
        assertEquals(Request.RequestState.CANCELED, request3.value().state);
    }

    @Test
    void testOverride() {
        send(SOURCE, 0, 1, 1L);

        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        send(OVERRIDE1, 0, Long.parseLong(ds1outNew.refId()), "111");
        send(OVERRIDE2, 0, Long.parseLong(ds2outNew.refId()), "222");
        send(OVERRIDE3, 0, Long.parseLong(ds3outNew.refId()), "333");

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1+111", ds1outNew.payload());

        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>1|1+222", ds2outAmend.payload());

        Outgoing ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("amend", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("2", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("rd3>1|1+333", ds3outAmend.payload());
    }
}
