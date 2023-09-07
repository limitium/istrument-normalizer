package com.limitium.gban.instrumentnormalizer;

import com.limitium.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBInstrumentLookuped;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.limitium.gban.flatbufferstooling.communication.FlatbuffersSerde;
import com.limitium.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Serdes;

public class Topics {

    public static final String UPSTREAM_PATTERN = "gba.upstream.domain.*.*.normalizeInstrument";
    public static final String REDUCE_LOOKUP_INSTRUMENT_TOPIC = "gba.instrument.internal.reduce.lookupInstrument";
    public static final String LOOKUP_INSTRUMENT_TOPIC = "gba.instrument.internal.lookupInstrument";
    public static final String INSTRUMENT_LOOKUPED_TOPIC = "gba.instrument.internal.instrumentLookuped";
    public static final String DLQ_TOPIC = "gba.instrument.internal.dlq";

    public static Topic<String, FBNormalizeInstrument> UPSTREAM;
    public static Topic<String, FBLookupInstrument> REDUCE_LOOKUP_INSTRUMENT;
    public static Topic<String, FBLookupInstrument> LOOKUP_INSTRUMENT;
    public static Topic<String, FBInstrumentLookuped> INSTRUMENT_LOOKUPED;
    public static Topic<String, FBNormalizeInstrument> DLQ; //DLQ message must be combined from incoming message and error information

    static {
        FlatbuffersSerde<FBNormalizeInstrument> normalizeInstrumentSerde =
                new FlatbuffersSerde<>(FBNormalizeInstrument.class);

        FlatbuffersSerde<FBInstrumentLookuped> instrumentLookupedSerde =
                new FlatbuffersSerde<>(FBInstrumentLookuped.class);

        FlatbuffersSerde<FBLookupInstrument> lookupInstrumentSerde =
                new FlatbuffersSerde<>(FBLookupInstrument.class);

        Topics.UPSTREAM =
                new Topic<>(
                        UPSTREAM_PATTERN,
                        Serdes.String(),
                        normalizeInstrumentSerde);

        Topics.REDUCE_LOOKUP_INSTRUMENT =
                new Topic<>(
                        REDUCE_LOOKUP_INSTRUMENT_TOPIC,
                        Serdes.String(),
                        lookupInstrumentSerde);

        Topics.LOOKUP_INSTRUMENT =
                new Topic<>(
                        LOOKUP_INSTRUMENT_TOPIC,
                        Serdes.String(),
                        lookupInstrumentSerde);

        Topics.INSTRUMENT_LOOKUPED =
                new Topic<>(
                        INSTRUMENT_LOOKUPED_TOPIC,
                        Serdes.String(),
                        instrumentLookupedSerde);

        Topics.DLQ =
                new Topic<>(
                        DLQ_TOPIC,
                        Serdes.String(),
                        normalizeInstrumentSerde); // Generic DLQ serde required
    }
}
