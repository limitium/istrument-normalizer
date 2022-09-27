package com.bnpparibas.gban.usstreetprocessor;

import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.bnpparibas.gban.usstreetprocessor.external.ClientKeeper;
import com.bnpparibas.gban.usstreetprocessor.external.InstrumentKeeper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class UsStreetProcessor implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(UsStreetProcessor.class);

    public static final String EXECUTION_REPORT_SOURCE = "us_execution_report_source";
    public static final String TIGER_REPLY_SOURCE = "tiger_reply_source";

    public static final String STORE_EXECUTION_REPORTS = "execution_reports_store";
    public static final String STORE_TIGER_ALLOCATIONS = "tiger_allocations_store";
    public static final String STORE_EXECUTION_ALLOCATION_REF = "execution_allocation_ref_store";

    public static final String EXECUTION_REPORT_PROCESSOR = "us_execution_report_processor";
    public static final String TIGER_REPLY_PROCESSOR = "tiger_reply_processor";

    public static final String TIGER_ALLOCATION_SINK = "tiger_allocation_sink";


    @Autowired
    InstrumentKeeper instrumentKeeper;

    @Autowired
    ClientKeeper clientKeeper;

    @Override
    public void configureTopology(Topology topology) {


        StoreBuilder<KeyValueStore<Long, UsStreetExecution>> storedExecutions =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_EXECUTION_REPORTS),
                        Serdes.Long(),
                        TigerReply.Topics.EXECUTION_REPORTS.valueSerde
                );

        StoreBuilder<KeyValueStore<Long, TigerAllocation>> storedTrades =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_TIGER_ALLOCATIONS),
                        Serdes.Long(),
                        new JsonSerde<>()
                );

        StoreBuilder<KeyValueStore<Long, Long>> storedRefs =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_EXECUTION_ALLOCATION_REF),
                        Serdes.Long(),
                        Serdes.Long()
                );


        topology
                .addSource(EXECUTION_REPORT_SOURCE, TigerReply.Topics.EXECUTION_REPORTS.keySerde.deserializer(), TigerReply.Topics.EXECUTION_REPORTS.valueSerde.deserializer(), TigerReply.Topics.EXECUTION_REPORTS.topic)
                .addSource(TIGER_REPLY_SOURCE, TigerReply.Topics.TIGER_REPLY.keySerde.deserializer(), TigerReply.Topics.TIGER_REPLY.valueSerde.deserializer(), TigerReply.Topics.TIGER_REPLY.topic)

                // add two processors one per source
                .addProcessor(EXECUTION_REPORT_PROCESSOR, () -> new ExecutionReportProcessor(instrumentKeeper, clientKeeper), EXECUTION_REPORT_SOURCE)
                .addProcessor(TIGER_REPLY_PROCESSOR, TigerReplyProcessor::new, TIGER_REPLY_SOURCE)

                // add stores to both processors
                .addStateStore(storedExecutions, EXECUTION_REPORT_PROCESSOR)
                .addStateStore(storedTrades, EXECUTION_REPORT_PROCESSOR, TIGER_REPLY_PROCESSOR)
                .addStateStore(storedRefs, EXECUTION_REPORT_PROCESSOR)

                .addSink(TIGER_ALLOCATION_SINK, TigerReply.Topics.TIGER_BOOKED.topic,
                        TigerReply.Topics.BOOK_ENRICHED.keySerde.serializer(),
                        TigerReply.Topics.BOOK_ENRICHED.valueSerde.serializer(),
                        EXECUTION_REPORT_PROCESSOR);

        //add DQL sink and use it
    }


    private static class ExecutionReportProcessor implements org.apache.kafka.streams.processor.api.Processor<Long, UsStreetExecution, Long, String> {
        private final InstrumentKeeper instrumentKeeper;
        private final ClientKeeper clientKeeper;
        private KeyValueStore<Long, UsStreetExecution> executionReportStore;
        private KeyValueStore<Long, TigerAllocation> tigerAllocationsStore;
        private KeyValueStore<Long, Long> executionAllocationRefsStore;
        private ProcessorContext<Long, String> context;

        public ExecutionReportProcessor(InstrumentKeeper instrumentKeeper, ClientKeeper clientKeeper) {
            this.instrumentKeeper = instrumentKeeper;
            this.clientKeeper = clientKeeper;
        }

        @Override
        public void init(ProcessorContext<Long, String> context) {
            org.apache.kafka.streams.processor.api.Processor.super.init(context);
            this.context = context;
            executionReportStore = context.getStateStore(STORE_EXECUTION_REPORTS);
            tigerAllocationsStore = context.getStateStore(STORE_TIGER_ALLOCATIONS);
            executionAllocationRefsStore = context.getStateStore(STORE_EXECUTION_ALLOCATION_REF);
        }


        @Override
        public void process(Record<Long, UsStreetExecution> record) {
            UsStreetExecution usStreetExecution = record.value();
            logger.info("Store:{},{}", record.key(), usStreetExecution);

            executionReportStore.put(record.key(), usStreetExecution);

            switch (usStreetExecution.state) {
                case "NEW" -> {
                    logger.info("NEW,EXEC:{}", usStreetExecution.executionId);

                    TigerAllocation tigerAllocation = new TigerAllocation();
                    //generate id from sequencer with right partition

                    tigerAllocation.state = TigerAllocation.State.NEW_PENDING;
                    tigerAllocation.executionId = usStreetExecution.executionId;

                    fillFromExecution(tigerAllocation, usStreetExecution);

                    enrichInstrumentData(tigerAllocation, usStreetExecution);
                    enrichClientData(tigerAllocation, usStreetExecution);

                    tigerAllocationsStore.put(tigerAllocation.id, tigerAllocation);
                    executionAllocationRefsStore.put(usStreetExecution.executionId, tigerAllocation.id);

                    logger.info("NEW,ALLOC:{}", tigerAllocation.id);
                    context.forward(record.withValue(convertToCSV(tigerAllocation)), TIGER_ALLOCATION_SINK);
                }
                case "CANCEL" -> {
                    logger.info("CANCEL,EXEC:{}", usStreetExecution.executionId); //@todo: should be ref_exec_id

                    long tigerAllocationId = executionAllocationRefsStore.get(usStreetExecution.executionId);
                    if (tigerAllocationId > 0) {
                        TigerAllocation tigerAllocation = tigerAllocationsStore.get(tigerAllocationId);
                        try {
                            tigerAllocation.state.transferTo(TigerAllocation.State.CANCEL_PENDING);

                            logger.info("CANCEL,ALLOC:{}", tigerAllocation.id);
                            context.forward(record.withValue(convertToCSV(tigerAllocation)), TIGER_ALLOCATION_SINK);
                        } catch (TigerAllocation.State.WrongStateTransitionException e) {
                            logger.error("Wrong state change request", e);
                        }
                    }
                }
                default -> throw new RuntimeException("Wrong state");
            }
        }

        private void enrichClientData(TigerAllocation tigerAllocation, UsStreetExecution usStreetExecution) {
            ClientKeeper.Book book = clientKeeper.getBookById(usStreetExecution.bookId);
            //@todo: fill allocation
        }

        private void enrichInstrumentData(TigerAllocation tigerAllocation, UsStreetExecution usStreetExecution) {
            InstrumentKeeper.Equity equity = instrumentKeeper.getById(usStreetExecution.instrumentId);
            //@todo: fill allocation
        }

        private void fillFromExecution(TigerAllocation tigerAllocation, UsStreetExecution usStreetExecution) {
            //@todo: copy data
        }

        private String convertToCSV(TigerAllocation tigerAllocation) {
            return "1,2,3,,5";
        }
    }

    private static class TigerReplyProcessor implements Processor<Long, TigerReply, Object, Object> {
        private KeyValueStore<Long, TigerAllocation> tigerAllocationsStore;

        @Override
        public void process(Record<Long, TigerReply> record) {
            TigerReply tigerReply = record.value();

            logger.info("REPLY,{}->{}", tigerReply.tradeId, tigerReply.state);
            TigerAllocation tigerAllocation = tigerAllocationsStore.get(tigerReply.tradeId);

            if (tigerAllocation != null) {
                try {
                    tigerAllocation.state.transferTo(tigerReply.state);

                    tigerAllocationsStore.put(tigerAllocation.id, tigerAllocation);
                } catch (TigerAllocation.State.WrongStateTransitionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                logger.warn("Allocation not found");
            }
        }
    }
}
