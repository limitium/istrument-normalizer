package com.bnpparibas.gban.usstreetprocessor;

import com.bnpparibas.gban.bibliotheca.sequencer.Namespace;
import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import com.bnpparibas.gban.usstreetprocessor.common.Topics;
import com.bnpparibas.gban.usstreetprocessor.common.messages.ReplyCode;
import com.bnpparibas.gban.usstreetprocessor.common.messages.TigerReply;
import com.bnpparibas.gban.usstreetprocessor.common.messages.UsStreetExecution;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;

import static com.bnpparibas.gban.usstreetprocessor.UsStreetController.TigerReplyCoProcessor.TIGER_TIMESTAMP_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                Topics.BOOK_ENRICHED_TOPIC,
                Topics.TIGER_REPLY_CSV_TOPIC,
                Topics.TIGER_REPLY_TOPIC
        },
        consumers = {
                Topics.TIGER_REPLY_TOPIC,
                Topics.BOOK_TIGER_TOPIC
        })
class UsStreetApplicationControllerTests extends BaseKStreamApplicationTests {

    @Test
    void newExecutionReportReceived() {
        //@todo: send UsStreetExecution
        //@todo: check outgoing Tiger CSV request
        //@todo: check outgoing TigerAllocation store changelog topic for NEW_PENDING state
    }

    @Test
    void tigerReplyReceived() {
        //@todo: send new UsStreetExecution
        //@todo: check outgoing Tiger CSV request
        //@todo: check outgoing TigerAllocation store changelog topic for NEW_PENDING state
        //@todo: send ack TigerReply
        //@todo: check outgoing TigerAllocation store changelog topic for NEW_ACK state
    }

    @Test
    void testSendNewExecutionAndCancelIt() {
        UsStreetExecution execution = createTestExecution();
        sendExecution(execution);

        String csv = readMessageFromBookTiger();
        assertEquals("NEW_PENDING", getExecutionStatus(csv));
        assertEquals(0, getVersion(csv));

        execution.state = "CANCEL";
        sendExecution(execution);

        csv = readMessageFromBookTiger();
        assertEquals("CANCEL_PENDING", getExecutionStatus(csv));
        assertEquals(1, getVersion(csv));
    }
    private UsStreetExecution createTestExecution() {
        UsStreetExecution execution = new UsStreetExecution();
        execution.portfolioCode = "123";
        execution.securityId = "AAA";
        execution.instrumentId = 1000;
        execution.state = "NEW";
        execution.bookId = 0;
        return execution;
    }

    private void sendExecution(UsStreetExecution execution) {
        send(Topics.BOOK_ENRICHED, execution.executionId, execution);
    }

    private String readMessageFromBookTiger() {
        return waitForRecordFrom(Topics.BOOK_TIGER).value();
    }

    private long getExecutionId(String csv) {
        return Long.parseLong(csv.split(",")[2]);
    }

    private String getExecutionStatus(String csv) {
        return csv.split(",")[5];
    }

    private long getVersion(String csv) {
        return Long.parseLong(csv.split(",")[4]);
    }



//    @Test
    void tigerCSVReplyReceived() {
        testConverterAndCopartitioning(0);
        testConverterAndCopartitioning(1);
    }

    private void testConverterAndCopartitioning(int partition) {
        long allocationId = new Sequencer(() -> 0, Namespace.US_STREET_CASH_EQUITY, partition).getNext();
        int allocationVersion = 3;
        ReplyCode replyCode = ReplyCode.OK;
        String ackTimestamp = "20220603-11:05:12";


        String csvReply = generateCSVReply(allocationId, allocationVersion, replyCode, ackTimestamp);
        send(Topics.TIGER_REPLY_CSV, 0L, csvReply);

        ConsumerRecord<Long, TigerReply> record = waitForRecordFrom(Topics.TIGER_REPLY);

        Long keyAllocationId = record.key();
        TigerReply tigerReply = record.value();

        assertEquals(allocationId, keyAllocationId);
        assertEquals(allocationId, tigerReply.allocationId);
        assertEquals(allocationVersion, tigerReply.version);
        assertEquals(replyCode, tigerReply.replyCode);
        assertEquals(partition, record.partition());

        assertEquals(ackTimestamp, tigerReply.ackTimestamp.format(DateTimeFormatter.ofPattern(TIGER_TIMESTAMP_FORMAT)));
    }

    private String generateCSVReply(long allocationId, int allocationVersion, ReplyCode replyCode, String ackTimestamp) {
        return replyCode.code + ",0-" + UsStreetController.US_STREET_PROCESSOR_APP_NAME + "-" + allocationId + "-" + allocationVersion + "," + ackTimestamp;
    }
}
