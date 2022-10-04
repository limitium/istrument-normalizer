package com.bnpparibas.gban.usstreetprocessor;

import com.bnpparibas.gban.bibliotheca.sequencer.Namespace;
import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
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

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
public class UsStreetProcessor implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(UsStreetProcessor.class);

    public static final String US_STREET_PROCESSOR_APP_NAME = "US_STREET_PROCESSOR";


    public static final String EXECUTION_REPORT_SOURCE = "us_execution_report_source";
    public static final String TIGER_REPLY_CSV_SOURCE = "tiger_reply_csv_source";
    public static final String TIGER_REPLY_SOURCE = "tiger_reply_source";

    public static final String STORE_EXECUTION_REPORTS = "execution_reports_store";
    public static final String STORE_TIGER_ALLOCATIONS = "tiger_allocations_store";
    public static final String STORE_EXECUTION_ALLOCATION_REF = "execution_allocation_ref_store";

    public static final String EXECUTION_REPORT_PROCESSOR = "us_execution_report_processor";
    public static final String TIGER_REPLY_COPROCESSOR = "tiger_reply_co_processor";
    public static final String TIGER_REPLY_PROCESSOR = "tiger_reply_processor";

    public static final String TIGER_REPLY_SINK = "tiger_reply_sink";
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
                        Topics.EXECUTION_REPORTS.valueSerde
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
                .addSource(EXECUTION_REPORT_SOURCE, Topics.BOOK_ENRICHED.keySerde.deserializer(), Topics.BOOK_ENRICHED.valueSerde.deserializer(), Topics.BOOK_ENRICHED.topic)
                .addSource(TIGER_REPLY_CSV_SOURCE, Topics.TIGER_REPLY_CSV.keySerde.deserializer(), Topics.TIGER_REPLY_CSV.valueSerde.deserializer(), Topics.TIGER_REPLY_CSV.topic)
                .addSource(TIGER_REPLY_SOURCE, Topics.TIGER_REPLY.keySerde.deserializer(), Topics.TIGER_REPLY.valueSerde.deserializer(), Topics.TIGER_REPLY.topic)

                .addProcessor(EXECUTION_REPORT_PROCESSOR, () -> new ExecutionReportProcessor(instrumentKeeper, clientKeeper), EXECUTION_REPORT_SOURCE)
                .addProcessor(TIGER_REPLY_COPROCESSOR, TigerReplyCoProcessor::new, TIGER_REPLY_CSV_SOURCE)
                .addProcessor(TIGER_REPLY_PROCESSOR, TigerReplyProcessor::new, TIGER_REPLY_SOURCE)


                .addStateStore(storedExecutions, EXECUTION_REPORT_PROCESSOR)
                .addStateStore(storedTrades, EXECUTION_REPORT_PROCESSOR, TIGER_REPLY_PROCESSOR)
                .addStateStore(storedRefs, EXECUTION_REPORT_PROCESSOR)

                .addSink(TIGER_ALLOCATION_SINK,
                        Topics.BOOK_TIGER.topic,
                        Topics.BOOK_TIGER.keySerde.serializer(),
                        Topics.BOOK_TIGER.valueSerde.serializer(),
                        EXECUTION_REPORT_PROCESSOR)

                .addSink(TIGER_REPLY_SINK,
                        Topics.TIGER_REPLY.topic,
                        Topics.TIGER_REPLY.keySerde.serializer(),
                        Topics.TIGER_REPLY.valueSerde.serializer(),
                        (topic, allocationId, reply, numPartitions) -> Sequencer.getPartition(allocationId),
                        TIGER_REPLY_COPROCESSOR); //Partition is part of sequence id

        //add DQL sink and use it
    }


    /**
     * Accepts a fully enriched {@link UsStreetExecution}, creates {@link TigerAllocation} based on it or cancel a previous on.
     * Converts {@link TigerAllocation} into CSV representation and sends to Tiger
     */
    private static class ExecutionReportProcessor implements Processor<Long, UsStreetExecution, Long, String> {
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
                    tigerAllocation.id = generateNextId();

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

        /**
         * Generate id with a partition information stored in sequence. Which will be used later for stream copartition
         *
         * @return new uniq sequence
         */
        private long generateNextId() {
            int partition = context.recordMetadata()
                    //Shouldn't be thrown in fact from `process` call
                    .orElseThrow(() -> new RuntimeException("Empty stream record metadata, unable to get partition for Sequencer"))
                    .partition();

            return new Sequencer(() -> context.currentStreamTimeMs(), Namespace.US_STREET_CASH_EQUITY, partition)
                    .getNext();
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

    /**
     * Accepts parsed {@link TigerReply} and controls {@link TigerAllocation} state according to {@link ReplyCode}
     */
    private static class TigerReplyProcessor implements Processor<Long, TigerReply, Object, Object> {
        private KeyValueStore<Long, TigerAllocation> tigerAllocationsStore;

        @Override
        public void init(ProcessorContext<Object, Object> context) {
            tigerAllocationsStore = context.getStateStore(STORE_TIGER_ALLOCATIONS);
        }

        @Override
        public void process(Record<Long, TigerReply> record) {
            TigerReply tigerReply = record.value();

            logger.info("REPLY,{}->{}", tigerReply.allocationId, tigerReply.replyCode.code);
            TigerAllocation tigerAllocation = tigerAllocationsStore.get(tigerReply.allocationId);

            if (tigerAllocation != null) {
                try {
                    TigerAllocation.State newState = TigerAllocation.State.NEW_PENDING;
                    //@todo: check version, should be skipped? Check current behavior
                    //@todo: generateNew state
                    //@todo: set reason on nack
                    tigerAllocation.state.transferTo(newState);

                    tigerAllocationsStore.put(tigerAllocation.id, tigerAllocation);
                } catch (TigerAllocation.State.WrongStateTransitionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                logger.warn("Allocation not found");
            }
        }
    }

    /**
     * Accepts raw reply in CSV format and coverts it to allocationId, {@link TigerReply} records.
     * Key with allocationId is used in sink in custom partitioner
     */
    public static class TigerReplyCoProcessor implements Processor<String, String, Long, TigerReply> {

        private static final int COLUMNS_IN_REPLY = 3;
        private static final int CORRELATION_PARTS = 4;
        public static final String TIGER_TIMESTAMP_FORMAT = "yyyyMMdd-HH:mm:ss";
        private ProcessorContext<Long, TigerReply> context;

        @Override
        public void init(ProcessorContext<Long, TigerReply> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            logger.info("RCVD:{}",record.value());
            ParsedReply parsedReply = parseReply(record.value());

            if (US_STREET_PROCESSOR_APP_NAME.equals(parsedReply.applicationId)) {
                TigerReply tigerReply = new TigerReply(parsedReply.allocationId, parsedReply.allocationVersion, parsedReply.ackTimestamp, parsedReply.replyCode);
                context.forward(new Record<Long, TigerReply>(parsedReply.allocationId, tigerReply, record.timestamp(), record.headers()), TIGER_REPLY_SINK);
            } else {
                logger.info("SKIP app:{}", parsedReply.applicationId);
            }
        }

        /**
         * Converts reply raw CSV representation into {@link ParsedReply}.
         * Might throw {@link ParseException} if something goes wrong.
         *
         * @param csv raw tiger reply
         * @return
         */
        private ParsedReply parseReply(String csv) {
            String[] columns = csv.split(",");
            if (columns.length != COLUMNS_IN_REPLY) {
                throw new ReplyParseException("Unexpected number of columns, must be " + COLUMNS_IN_REPLY);
            }

            String correlationId = columns[1];
            String[] correlationParts = correlationId.split("-");
            if (correlationParts.length != CORRELATION_PARTS) {
                throw new ReplyParseException("Unexpected number of correlation parts `" + correlationId + CORRELATION_PARTS);
            }

            try {
                ReplyCode replyCode = ReplyCode.getReplyCodeBy(columns[0]);

                int wtf = Integer.parseInt(correlationParts[0]);
                String applicationId = correlationParts[1];
                long allocationId = Long.parseLong(correlationParts[2]);
                int allocationVersion = Integer.parseInt(correlationParts[3]);

                LocalDateTime ackTimestamp = LocalDateTime
                        .parse(columns[2], DateTimeFormatter.ofPattern(TIGER_TIMESTAMP_FORMAT, Locale.US));

                return new ParsedReply(replyCode, wtf, applicationId, allocationId, allocationVersion, ackTimestamp);
            } catch (NumberFormatException e) {
                throw new ReplyParseException(e);
            }
        }

        /**
         * Intermediate structure with all information available in reply
         */
        public static class ParsedReply {
            ReplyCode replyCode;

            //@todo: Figure out what 1st number in ExternalSrcID mean
            int wtf;
            String applicationId;
            long allocationId;
            int allocationVersion;

            LocalDateTime ackTimestamp;

            public ParsedReply(ReplyCode replyCode, int wtf, String applicationId, long allocationId, int allocationVersion, LocalDateTime ackTimestamp) {
                this.replyCode = replyCode;
                this.wtf = wtf;
                this.applicationId = applicationId;
                this.allocationId = allocationId;
                this.allocationVersion = allocationVersion;
                this.ackTimestamp = ackTimestamp;
            }
        }

        /**
         * Common exception for CSV parse process
         */
        public static class ReplyParseException extends RuntimeException {

            public ReplyParseException(String reason) {
                super(reason);
            }

            public ReplyParseException(Exception exception) {
                super(exception);
            }
        }
    }
}
