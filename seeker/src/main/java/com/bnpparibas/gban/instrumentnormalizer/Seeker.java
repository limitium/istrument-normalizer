package com.limitium.gban.instrumentnormalizer;

import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.streams.StreamsBuilder;
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
    InstrumentSeeker instrumentSeeker;

    @Override
    public void configureBuilder(StreamsBuilder builder) {
        builder
                .stream(Topics.LOOKUP_INSTRUMENT.topic, Consumed.with(Topics.LOOKUP_INSTRUMENT.keySerde, Topics.LOOKUP_INSTRUMENT.valueSerde))
                .peek((securityId, lookupInstrument) -> logger.info("{}", lookupInstrument.securityId()))
                .mapValues((securityId, lookupInstrument) -> instrumentSeeker.lookupInstrument(lookupInstrument))
                .peek((securityId, lookupedIstrument) -> logger.info("resolved:{}", 1/*lookupedIstrument.instrumentId*/))
                .to(Topics.INSTRUMENT_LOOKUPED.topic, Produced.with(Topics.INSTRUMENT_LOOKUPED.keySerde, Topics.INSTRUMENT_LOOKUPED.valueSerde))
        ;
    }

}
