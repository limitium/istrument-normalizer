package extb.gba.instrument.normalizer;


import extb.gba.instrument.normalizer.external.InstrumentKeeper;
import extb.gba.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class Receiver implements KStreamInfraCustomizer.KStreamDSLBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    @Autowired
    InstrumentKeeper instrumentKeeper;
    @Override
    public void configureBuilder(StreamsBuilder builder) {

        builder
                //todo: as entry point of component meta for tracing should be added to headers
                .stream(Topics.UPSTREAM.topic, Consumed.with(Topics.UPSTREAM.keySerde, Topics.UPSTREAM.valueSerde))
                //Enriches instrumentId via gRPC to Instrument keeper
                .peek((securityId, instrumental) -> instrumentKeeper.enrichInstrument(instrumental), Named.as("enrich_instrument"))
                .split()
                //Empty enrichment goes to instrument seeker
                .branch((securityId, instrumental) -> instrumental.instrumentId == 0,
                        Branched.withConsumer(kStream -> kStream
                                //todo: add to log raw message type and public id
                                .peek((securityId, instrumental) -> logger.info("{},{}->X", instrumental.executionId, instrumental.securityId))
                                //todo: Generic message with byte[] payload should be introduced
                                .to(Topics.INSTRUMENT_MISSED.topic, Produced.with(Topics.INSTRUMENT_MISSED.keySerde, Topics.INSTRUMENT_MISSED.valueSerde)))
                )
                .defaultBranch(Branched.withConsumer(kStream -> kStream
                        //Enriched executions partitioned by public instrumentId
                        .map((securityId, instrumental) -> KeyValue.pair(instrumental.instrumentId, instrumental))
                        .peek((securityId, instrumental) -> logger.info("{},{}->{}", instrumental.executionId, instrumental.securityId, instrumental.instrumentId))
                        .to(Topics.INSTRUMENT_ENRICHED.topic, Produced.with(Topics.INSTRUMENT_ENRICHED.keySerde, Topics.INSTRUMENT_ENRICHED.valueSerde))
                ));
    }
}
