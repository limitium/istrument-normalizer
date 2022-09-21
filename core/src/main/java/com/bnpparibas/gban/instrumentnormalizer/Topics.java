package com.bnpparibas.gban.instrumentnormalizer;

import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBInstrumentLookuped;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.bnpparibas.gban.flatbufferstooling.communication.FlatbuffersSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class Topics {
    public static class Topic<K, V> {
        public String topic;
        public Serde<K> keySerde;
        public Serde<V> valueSerde;

        public Topic(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
            this.topic = topic;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

    }

    public static Topic<String, FBNormalizeInstrument> UPSTREAM;
    public static Topic<String, FBLookupInstrument> REDUCE_LOOKUP_INSTRUMENT;
    public static Topic<String, FBLookupInstrument> LOOKUP_INSTRUMENT;
    public static Topic<String, FBInstrumentLookuped> INSTRUMENT_LOOKUPED;
    public static Topic<String, FBNormalizeInstrument> DLQ; //DLQ message must be combined from incoming message and error information

    static {

        FlatbuffersSerde<FBNormalizeInstrument> normalizeInstrumentSerde = new FlatbuffersSerde<>(FBNormalizeInstrument.class);
        FlatbuffersSerde<FBInstrumentLookuped> instrumentLookupedSerde = new FlatbuffersSerde<>(FBInstrumentLookuped.class);
        FlatbuffersSerde<FBLookupInstrument> lookupInstrumentSerde = new FlatbuffersSerde<>(FBLookupInstrument.class);

        Topics.UPSTREAM = new Topic<>("gba.upstream.domain.*.*.normalizeInstrument", Serdes.String(), normalizeInstrumentSerde);
        Topics.REDUCE_LOOKUP_INSTRUMENT = new Topic<>("gba.instrument.internal.reduce.lookupInstrument", Serdes.String(), lookupInstrumentSerde);
        Topics.LOOKUP_INSTRUMENT = new Topic<>("gba.instrument.internal.lookupInstrument", Serdes.String(), lookupInstrumentSerde);
        Topics.INSTRUMENT_LOOKUPED = new Topic<>("gba.instrument.internal.instrumentLookuped", Serdes.String(), instrumentLookupedSerde);


        Topics.DLQ = new Topic<>("gba.instrument.internal.dlq", Serdes.String(), normalizeInstrumentSerde); //Generic DLQ serde required


    }
}
