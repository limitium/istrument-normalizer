package com.bnpparibas.gban.instrumentnormalizer;

import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBInstrumentLookuped;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                Topics.LOOKUP_INSTRUMENT_TOPIC,
                Topics.INSTRUMENT_LOOKUPED_TOPIC
        },
        consumers = {Topics.INSTRUMENT_LOOKUPED_TOPIC})
public class SeekerTests extends BaseKStreamApplicationTests {

    @Test
    void seekTest() {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        String securityId = "123123";
        int securityOffset = builder.createString(securityId);
        int fBLookupInstrumentOffset =
                FBLookupInstrument.createFBLookupInstrument(builder, securityOffset);
        builder.finish(fBLookupInstrumentOffset);
        FBLookupInstrument fblookupinstr =
                FBLookupInstrument.getRootAsFBLookupInstrument(
                        ByteBuffer.wrap(builder.sizedByteArray()));
        send(
                Topics.LOOKUP_INSTRUMENT,
                securityId,
                fblookupinstr);
        ConsumerRecord<String, FBInstrumentLookuped> record =
                waitForRecordFrom(Topics.INSTRUMENT_LOOKUPED);

        assertEquals(securityId, record.value().securityId());
    }
}
