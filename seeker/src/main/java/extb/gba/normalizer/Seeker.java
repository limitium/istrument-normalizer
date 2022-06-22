package extb.gba.normalizer;

import extb.gba.instrument.normalizer.Topics;
import extb.gba.instrument.normalizer.external.InstrumentKeeper;
import extb.gba.instrument.normalizer.external.InstrumentSeeker;
import extb.gba.instrument.normalizer.messages.SeekResponse;
import extb.gba.instrument.normalizer.messages.UpsertInstrument;
import extb.gba.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Seeker implements KStreamInfraCustomizer.KStreamDSLBuilder {

    private static final Logger logger = LoggerFactory.getLogger(Seeker.class);

    @Autowired
    InstrumentKeeper instrumentKeeper;
    @Autowired
    InstrumentSeeker instrumentSeeker;

    @Override
    public void configureBuilder(StreamsBuilder builder) {
        builder
                .stream(Topics.INSTRUMENT_SEEK_REQUEST.topic, Consumed.with(Topics.INSTRUMENT_SEEK_REQUEST.keySerde, Topics.INSTRUMENT_SEEK_REQUEST.valueSerde))
                .peek((securityId, seekRequest) -> logger.info("{}", seekRequest.securityId))
                .mapValues((securityId, seekRequest) -> instrumentSeeker.lookupInstrument(seekRequest))
                .split()
                .branch((securityId, seekResponse) -> seekResponse.status == SeekResponse.Status.SUCCESS,
                        Branched.withConsumer(kStream -> kStream
                                .mapValues(Seeker::transformSeekResponseToInstrument)
                                .mapValues((securityId, upsertInstrument) -> instrumentKeeper.upsert(upsertInstrument))
                                .peek((securityId, instrument) -> logger.info("resolved:{}", instrument.instrumentId))
                                .to(Topics.INSTRUMENT_UPDATED.topic, Produced.with(Topics.INSTRUMENT_UPDATED.keySerde, Topics.INSTRUMENT_UPDATED.valueSerde))
                        )
                )
                .defaultBranch(Branched.withConsumer(kStream -> kStream
                        //todo: create unresolved topic
                        .peek((securityId, seekResponse) -> logger.info("failed:{}", seekResponse.reason))
                ));
    }

    private static UpsertInstrument transformSeekResponseToInstrument(String securityId, SeekResponse seekResponse) {
        UpsertInstrument upsertInstrument = new UpsertInstrument();
        upsertInstrument.ric = seekResponse.ric;
        return upsertInstrument;
    }
}
