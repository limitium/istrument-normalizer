package com.limitium.gban.instrumentnormalizer;

import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBInstrumentLookuped;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.google.flatbuffers.FlatBufferBuilder;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;


@Component
public class InstrumentSeeker {
    public FBInstrumentLookuped lookupInstrument(FBLookupInstrument lookupInstrument) {
        FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder();
        int securityIdOff = flatBufferBuilder.createString(lookupInstrument.securityId());

        FBInstrumentLookuped.startFBInstrumentLookuped(flatBufferBuilder);

        FBInstrumentLookuped.addSecurityId(flatBufferBuilder, securityIdOff);

        flatBufferBuilder.finish(FBInstrumentLookuped.endFBInstrumentLookuped(flatBufferBuilder));

        return FBInstrumentLookuped.getRootAsFBInstrumentLookuped(ByteBuffer.wrap(flatBufferBuilder.sizedByteArray()));
    }
}
