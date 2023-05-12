package com;

import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FbFixMsg;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FbTable1;
import com.bnpparibas.gban.flatbufferstooling.communication.NormalizeInstrument;
import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class NormalizeInstrumentTests {

    @Test
    public void mutateNestedInstrumentId() {
        FbFixMsg originalMsg = generateFb();
        FBNormalizeInstrument normalizeInstrument = NormalizeInstrument.wrapWithNormalizeInstrument(originalMsg, 1L, "asd", ".table1", "topic");

        NormalizeInstrument.mutateInstrumentId(normalizeInstrument, Long.MAX_VALUE);
        byte[] bytes = NormalizeInstrument.cutOriginalMessageFrom(normalizeInstrument);

        FbFixMsg mutatedMsg = FbFixMsg.getRootAsFbFixMsg(ByteBuffer.wrap(bytes));
        assertEquals(Long.MAX_VALUE, mutatedMsg.table1().instrumentId(), "Wrapped message wasn't mutated");
    }

    @Test
    public void mutateInstrumentId() {
        FbFixMsg originalMsg = generateFb();
        FBNormalizeInstrument normalizeInstrument = NormalizeInstrument.wrapWithNormalizeInstrument(originalMsg, 1L, "asd", ".", "topic");

        NormalizeInstrument.mutateInstrumentId(normalizeInstrument, Long.MAX_VALUE);
        byte[] bytes = NormalizeInstrument.cutOriginalMessageFrom(normalizeInstrument);

        FbFixMsg mutatedMsg = FbFixMsg.getRootAsFbFixMsg(ByteBuffer.wrap(bytes));
        assertEquals(Long.MAX_VALUE, mutatedMsg.instrumentId(), "Wrapped message wasn't mutated");
    }

    private FbFixMsg generateFb() {
        FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder().forceDefaults(true);

        int strOff = flatBufferBuilder.createString("abcd");

        int fbTable1 = FbTable1.createFbTable1(flatBufferBuilder, (byte) 3, Long.MIN_VALUE, strOff, (byte) 1, 3L);

        flatBufferBuilder.finish(FbFixMsg.createFbFixMsg(flatBufferBuilder, (byte) 1, fbTable1, Long.MIN_VALUE));

        byte[] bytes = flatBufferBuilder.sizedByteArray();
        return FbFixMsg.getRootAsFbFixMsg(ByteBuffer.wrap(bytes));
    }

}