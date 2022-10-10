package com.bnpparibas.gban.instrumentnormalizer;

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
                Topics.REDUCE_LOOKUP_INSTRUMENT_TOPIC,
                Topics.LOOKUP_INSTRUMENT_TOPIC
        },
        consumers = {Topics.LOOKUP_INSTRUMENT_TOPIC})
class ReducerTests extends BaseKStreamApplicationTests {

    @Test
    void reduceTest() {
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
                Topics.REDUCE_LOOKUP_INSTRUMENT,
                securityId,
                fblookupinstr);

        send(
                Topics.REDUCE_LOOKUP_INSTRUMENT,
                securityId,
                fblookupinstr);

        ConsumerRecord<String, FBLookupInstrument> record =
                waitForRecordFrom(Topics.LOOKUP_INSTRUMENT);

        assertEquals(securityId, record.value().securityId());

        ensureEmptyTopic(Topics.LOOKUP_INSTRUMENT);
    }
}