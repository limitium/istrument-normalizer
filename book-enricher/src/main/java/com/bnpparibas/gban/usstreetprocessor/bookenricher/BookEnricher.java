package com.bnpparibas.gban.usstreetprocessor.bookenricher;


import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.bnpparibas.gban.usstreetprocessor.common.Topics;
import com.bnpparibas.gban.usstreetprocessor.common.external.ClientKeeper;
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
public class BookEnricher implements KStreamInfraCustomizer.KStreamDSLBuilder {
    private static final Logger logger = LoggerFactory.getLogger(BookEnricher.class);

    @Autowired
    ClientKeeper clientKeeper;

    @Override
    public void configureBuilder(StreamsBuilder builder) {

        builder
                .stream(Topics.EXECUTION_REPORTS.topic, Consumed.with(Topics.EXECUTION_REPORTS.keySerde, Topics.EXECUTION_REPORTS.valueSerde))
                //Enriches book id via gRPC to Client keeper
                .peek((instrumentId, executionReport) -> clientKeeper.lookupBookByPortfolioCode(executionReport.portfolioCode), Named.as("enrich_book"))
                .split()
                //TODO: ask business for right strategy w8 or skip
                .branch((instrumentId, executionReport) -> executionReport.bookId == 0, //PrimitiveNulls.isNull()
                        Branched.withConsumer(kStream -> kStream //to DLQ implement custom BranchedConsumer with DLQ semantic
                                .peek((instrumentId, executionReport) -> logger.info("{},{}->X", executionReport.executionId, executionReport.portfolioCode))
                                //todo: introduce DLQRecord with incomingTopic, JSONdata, original byte array, message type, id, deser, reason, time, traceId
                                .mapValues((instrumentId, executionReport) -> executionReport)
                                .to(Topics.DLQ.topic, Produced.with(Topics.DLQ.keySerde, Topics.DLQ.valueSerde)))
                )
                .defaultBranch(Branched.withConsumer(kStream -> kStream
                        //Enriched executions partitioned by public instrumentId
                        .peek((instrumentId, executionReport) -> logger.info("{},{}->{}", executionReport.executionId, executionReport.portfolioCode, executionReport.bookId))
                        .to(Topics.BOOK_ENRICHED.topic, Produced.with(Topics.BOOK_ENRICHED.keySerde, Topics.BOOK_ENRICHED.valueSerde)))
                );
    }
}
