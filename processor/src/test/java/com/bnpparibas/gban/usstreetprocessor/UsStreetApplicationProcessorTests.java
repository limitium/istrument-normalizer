package com.bnpparibas.gban.usstreetprocessor;

import com.bnpparibas.gban.bibliotheca.sequencer.Namespace;
import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;

import static com.bnpparibas.gban.usstreetprocessor.UsStreetProcessor.TigerReplyCoProcessor.TIGER_TIMESTAMP_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "gba.us-street-processor.internal.bookEnriched",
                "gba.us-street-processor.domain.tigerCSVReplied",
                "gba.us-street-processor.internal.tigerReplied"
        },
        consumers = {
                "gba.us-street-processor.internal.tigerReplied"
        })
class UsStreetApplicationProcessorTests extends BaseKStreamApplicationTests {

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
    void tigerCSVReplyReceived() throws ParseException {
        testConverterAndCopartitioning(0);
        testConverterAndCopartitioning(1);
    }

    private void testConverterAndCopartitioning(int partition) throws ParseException {
        long allocationId = new Sequencer(() -> 0, Namespace.US_STREET_CASH_EQUITY, partition).getNext();
        int allocationVersion = 3;
        ReplyCode replyCode = ReplyCode.OK;
        String ackTimestamp = "20220603-11:05:12";


        String csvReply = generateCSVReply(allocationId, allocationVersion, replyCode, ackTimestamp);
        send("gba.us-street-processor.domain.tigerCSVReplied", "".getBytes(StandardCharsets.UTF_8), csvReply.getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom("gba.us-street-processor.internal.tigerReplied");


        Long keyAllocationId = Topics.TIGER_REPLY.keySerde.deserializer().deserialize("", record.key());
        TigerReply tigerReply = Topics.TIGER_REPLY.valueSerde.deserializer().deserialize("", record.value());

        assertEquals(allocationId, keyAllocationId);
        assertEquals(allocationId, tigerReply.allocationId);
        assertEquals(allocationVersion, tigerReply.version);
        assertEquals(replyCode, tigerReply.replyCode);
        assertEquals(partition, record.partition());

        assertEquals(ackTimestamp, tigerReply.ackTimestamp.format(DateTimeFormatter.ofPattern(TIGER_TIMESTAMP_FORMAT)));
    }

    private String generateCSVReply(long allocationId, int allocationVersion, ReplyCode replyCode, String ackTimestamp) {
        return replyCode.code + ",0-" + UsStreetProcessor.US_STREET_PROCESSOR_APP_NAME + "-" + allocationId + "-" + allocationVersion + "," + ackTimestamp;
    }
}
