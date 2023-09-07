package com.limitium.gban.instrumentnormalizer;


import com.limitium.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBInstrumentLookuped;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.seeker.flatbuffers.FBLookupInstrument;
import com.limitium.gban.flatbufferstooling.communication.NormalizeInstrument;
import com.limitium.gban.instrumentkeeper.client.InstrumentKeeperClient;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;


@Component
public class Receiver implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    public static final String SOURCE_NORMALIZE_INSTRUMENT = "normalize_instrument_source";
    public static final String SOURCE_LOOKUPED_INSTRUMENT = "lookuped_instrument_source";
    public static final String STORE_SHELVED_DATA_NAME = "shelved_messages_store";
    public static final String PROCESSOR_NORMALIZE_INSTRUMENT = "normalize_instrument_processor";
    public static final String PROCESSOR_LOOOKUPED_INSTRUMENT = "lookuped_instrument_processor";
    public static final String SINK_NORMALIZED_INSTRUMENT = "enriched_messages_sink";
    public static final String SINK_LOOKUP_INSTRUMENT = "lookup_instrument_sink";
    public static final String SINK_DLQ = "dlq_messages_sink";


    @Autowired
    InstrumentKeeperClient instrumentKeeper;

    @Override
    public void configureTopology(Topology topology) {
        StoreBuilder<KeyValueStore<String, FBNormalizeInstrument>> shelvedMissedData =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_SHELVED_DATA_NAME),
                        Serdes.String(),
                        Topics.UPSTREAM.valueSerde
                );

        topology
                .addSource(SOURCE_NORMALIZE_INSTRUMENT, Topics.UPSTREAM.keySerde.deserializer(), Topics.UPSTREAM.valueSerde.deserializer(), Pattern.compile(Topics.UPSTREAM.topic))
                .addSource(SOURCE_LOOKUPED_INSTRUMENT, Topics.INSTRUMENT_LOOKUPED.keySerde.deserializer(), Topics.INSTRUMENT_LOOKUPED.valueSerde.deserializer(), Topics.INSTRUMENT_LOOKUPED.topic)

                // add two processors one per source
                .addProcessor(PROCESSOR_NORMALIZE_INSTRUMENT, () -> new NormalizeInstrumentProcessor(instrumentKeeper), SOURCE_NORMALIZE_INSTRUMENT)
                .addProcessor(PROCESSOR_LOOOKUPED_INSTRUMENT, () -> new LookupedInstrumentProcessor(instrumentKeeper), SOURCE_LOOKUPED_INSTRUMENT)

                // add store to both processors
                .addStateStore(shelvedMissedData, PROCESSOR_NORMALIZE_INSTRUMENT, PROCESSOR_LOOOKUPED_INSTRUMENT)

                // add sinks to processors
                .addSink(SINK_NORMALIZED_INSTRUMENT,
                        (Long k, FBNormalizeInstrument v, RecordContext rc) -> v.egressTopic(),
                        new LongSerializer(),
                        this::originalMessageExtractor,
                        PROCESSOR_NORMALIZE_INSTRUMENT, PROCESSOR_LOOOKUPED_INSTRUMENT)
                .addSink(SINK_LOOKUP_INSTRUMENT, Topics.REDUCE_LOOKUP_INSTRUMENT.topic,
                        Topics.REDUCE_LOOKUP_INSTRUMENT.keySerde.serializer(),
                        Topics.REDUCE_LOOKUP_INSTRUMENT.valueSerde.serializer(),
                        PROCESSOR_NORMALIZE_INSTRUMENT)
                .addSink(SINK_DLQ, Topics.DLQ.topic,
                        Topics.DLQ.keySerde.serializer(),
                        Topics.DLQ.valueSerde.serializer(),
                        PROCESSOR_NORMALIZE_INSTRUMENT, PROCESSOR_LOOOKUPED_INSTRUMENT);
    }

    private byte[] originalMessageExtractor(String topic, FBNormalizeInstrument normalizeInstrument) {
        return NormalizeInstrument.cutOriginalMessageFrom(normalizeInstrument);
    }


    private static class NormalizeInstrumentProcessor implements Processor<String, FBNormalizeInstrument, Object, Object> {
        private final InstrumentKeeperClient instrumentKeeper;
        private KeyValueStore<String, FBNormalizeInstrument> shelvedMissedData;
        private ProcessorContext<Object, Object> context;

        public NormalizeInstrumentProcessor(InstrumentKeeperClient instrumentKeeper) {
            this.instrumentKeeper = instrumentKeeper;
        }

        @Override
        public void init(ProcessorContext<Object, Object> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore(STORE_SHELVED_DATA_NAME);
        }

        @Override
        public void process(Record<String, FBNormalizeInstrument> record) {
            String securityId = record.key();
            FBNormalizeInstrument normalizeInstrument = record.value();

            logger.info("msg_id:{},security_id:{}", normalizeInstrument.originalMessageId(), securityId);

            long instrumentId = instrumentKeeper.lookupIdBy(normalizeInstrument.securityId(),"","");

            if (instrumentId > 0) {

                //Instrument found, mutate original message

                NormalizeInstrument.mutateInstrumentId(normalizeInstrument, instrumentId);
                context.forward(record.withKey(instrumentId), SINK_NORMALIZED_INSTRUMENT);
            } else {
                //Instrument is absent, shelve message and make a new lookup

                String shelveKey = securityId + "_" + normalizeInstrument.originalMessageId();

                if (shelvedMissedData.get(shelveKey) != null) {
                    logger.error("key:{}, already shelved", shelveKey);
                    //TODO: add error data and wrap message
                    context.forward(record.withKey(instrumentId), SINK_DLQ);
                }

                shelvedMissedData.put(shelveKey, normalizeInstrument);
                context.forward(record.withValue(generateNormalizeInstrument(normalizeInstrument.securityId())), SINK_LOOKUP_INSTRUMENT);
            }
        }

        private FBLookupInstrument generateNormalizeInstrument(String securityId) {
            FlatBufferBuilder fbb = new FlatBufferBuilder();
            fbb.finish(FBLookupInstrument.createFBLookupInstrument(fbb, fbb.createString(securityId)));
            return FBLookupInstrument.getRootAsFBLookupInstrument(ByteBuffer.wrap(fbb.sizedByteArray()));
        }
    }

    private static class LookupedInstrumentProcessor implements Processor<String, FBInstrumentLookuped, Object, Object> {
        private final InstrumentKeeperClient instrumentKeeper;
        private KeyValueStore<String, FBNormalizeInstrument> shelvedMissedData;

        private ProcessorContext<Object, Object> context;

        public LookupedInstrumentProcessor(InstrumentKeeperClient instrumentKeeper) {
            this.instrumentKeeper = instrumentKeeper;
        }

        @Override
        public void init(ProcessorContext<Object, Object> context) {
            Processor.super.init(context);
            this.context = context;
            shelvedMissedData = context.getStateStore(STORE_SHELVED_DATA_NAME);
        }

        @Override
        public void process(Record<String, FBInstrumentLookuped> record) {
            FBInstrumentLookuped lookupedInstrument = record.value();
            String securityId = lookupedInstrument.securityId();

            long instrumentId = 0;

            //@todo: extract instrument from record;

            logger.info("Lookuped:{}->{}", securityId, instrumentId);

            if (instrumentId > 0) {
                try (KeyValueIterator<String, FBNormalizeInstrument> iterator = shelvedMissedData.prefixScan(securityId + "_", new StringSerializer())) {
                    while (iterator.hasNext()) {
                        KeyValue<String, FBNormalizeInstrument> normalizeInstrumentKeyValue = iterator.next();

                        //Instrument found, mutate original message

                        NormalizeInstrument.mutateInstrumentId(normalizeInstrumentKeyValue.value, instrumentId);
                        context.forward(record.withKey(instrumentId), SINK_NORMALIZED_INSTRUMENT);
                        shelvedMissedData.delete(normalizeInstrumentKeyValue.key);

                        //@todo: update instrument
                        //instrumentKeeper.upsert(lookupedInstrument.instrument());
                    }
                }
            } else {
                //@todo: instead of FBInstrumentLookuped send correct message
                context.forward(record, SINK_DLQ);
            }
        }
    }

    @Bean
    public static InstrumentKeeperClient instrumentKeeper(
            @Value("${instrument-keeper.host}") String host,
            @Value("${instrument-keeper.port}") int port) {
        return new InstrumentKeeperClient(host, port);
    }
}
