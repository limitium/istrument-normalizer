package com.bnpparibas.gban.usstreetcontroller;

import com.bnpparibas.gban.bibliotheca.sequencer.Namespace;
import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.bnpparibas.gban.usstreetcontroller.common.Topics;
import com.bnpparibas.gban.usstreetcontroller.common.external.ClientKeeper;
import com.bnpparibas.gban.usstreetcontroller.common.external.InstrumentKeeper;
import com.bnpparibas.gban.usstreetcontroller.common.messages.*;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
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
public class UsStreetController implements KStreamInfraCustomizer.KStreamTopologyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(UsStreetController.class);

    public static final String US_STREET_PROCESSOR_APP_NAME = "US_STREET_PROCESSOR";

    private static final String EXECUTION_REPORT_SOURCE = "us_execution_report_source";
    private static final String TIGER_REPLY_CSV_SOURCE = "tiger_reply_csv_source";
    private static final String TIGER_REPLY_SOURCE = "tiger_reply_source";

    private static final String EXECUTION_REPORTS_STORE = "execution_reports";
    private static final String TIGER_ALLOCATIONS_STORE = "tiger_allocations";
    private static final String EXECUTION_ALLOCATION_REF_STORE = "execution_allocation_ref";

    private static final String EXECUTION_REPORT_PROCESSOR = "us_execution_report_processor";
    private static final String TIGER_REPLY_COPROCESSOR = "tiger_reply_co_processor";
    private static final String TIGER_REPLY_PROCESSOR = "tiger_reply_processor";

    private static final String TIGER_REPLY_SINK = "tiger_reply_sink";
    private static final String TIGER_ALLOCATION_SINK = "tiger_allocation_sink";

    @Autowired private InstrumentKeeper instrumentKeeper;

    @Autowired private ClientKeeper clientKeeper;

    @Override
    public void configureTopology(Topology topology) {
        StoreBuilder<KeyValueStore<Long, UsStreetExecution>> executionStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EXECUTION_REPORTS_STORE),
                        Serdes.Long(),
                        Topics.EXECUTION_REPORTS.valueSerde);

        StoreBuilder<KeyValueStore<Long, TigerAllocation>> allocationStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(TIGER_ALLOCATIONS_STORE),
                        Serdes.Long(),
                        new JsonSerde<>(TigerAllocation.class));

        StoreBuilder<KeyValueStore<Long, Long>> refStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EXECUTION_ALLOCATION_REF_STORE),
                        Serdes.Long(),
                        Serdes.Long());

        topology.addSource(
                        EXECUTION_REPORT_SOURCE,
                        Topics.BOOK_ENRICHED.keySerde.deserializer(),
                        Topics.BOOK_ENRICHED.valueSerde.deserializer(),
                        Topics.BOOK_ENRICHED.topic)
                .addSource(
                        TIGER_REPLY_CSV_SOURCE,
                        Topics.TIGER_REPLY_CSV.keySerde.deserializer(),
                        Topics.TIGER_REPLY_CSV.valueSerde.deserializer(),
                        Topics.TIGER_REPLY_CSV.topic)
                .addSource(
                        TIGER_REPLY_SOURCE,
                        Topics.TIGER_REPLY.keySerde.deserializer(),
                        Topics.TIGER_REPLY.valueSerde.deserializer(),
                        Topics.TIGER_REPLY.topic)
                .addProcessor(
                        EXECUTION_REPORT_PROCESSOR,
                        () -> new ExecutionReportProcessor(instrumentKeeper, clientKeeper),
                        EXECUTION_REPORT_SOURCE)
                .addProcessor(
                        TIGER_REPLY_COPROCESSOR, TigerReplyCoProcessor::new, TIGER_REPLY_CSV_SOURCE)
                .addProcessor(TIGER_REPLY_PROCESSOR, TigerReplyProcessor::new, TIGER_REPLY_SOURCE)
                .addStateStore(executionStore, EXECUTION_REPORT_PROCESSOR)
                .addStateStore(allocationStore, EXECUTION_REPORT_PROCESSOR, TIGER_REPLY_PROCESSOR)
                .addStateStore(refStore, EXECUTION_REPORT_PROCESSOR)
                .addSink(
                        TIGER_ALLOCATION_SINK,
                        Topics.BOOK_TIGER.topic,
                        Topics.BOOK_TIGER.keySerde.serializer(),
                        Topics.BOOK_TIGER.valueSerde.serializer(),
                        EXECUTION_REPORT_PROCESSOR)
                .addSink(
                        TIGER_REPLY_SINK,
                        Topics.TIGER_REPLY.topic,
                        Topics.TIGER_REPLY.keySerde.serializer(),
                        Topics.TIGER_REPLY.valueSerde.serializer(),
                        (topic, allocationId, reply, numPartitions) ->
                                Sequencer.getPartition(allocationId),
                        TIGER_REPLY_COPROCESSOR); // Partition is part of sequence id

        // add DQL sink and use it
    }

    /**
     * Accepts a fully enriched {@link UsStreetExecution}, creates {@link TigerAllocation} based on
     * it or cancels the previous one. Converts {@link TigerAllocation} into CSV representation and
     * sends to Tiger.
     */
    private static class ExecutionReportProcessor
            implements Processor<Long, UsStreetExecution, Long, String> {

        private final InstrumentKeeper instrumentKeeper;

        private final ClientKeeper clientKeeper;

        private KeyValueStore<Long, UsStreetExecution> executionReportStore;

        private KeyValueStore<Long, TigerAllocation> tigerAllocationsStore;

        private KeyValueStore<Long, Long> executionAllocationRefsStore;

        private ProcessorContext<Long, String> context;

        public ExecutionReportProcessor(
                InstrumentKeeper instrumentKeeper, ClientKeeper clientKeeper) {
            this.instrumentKeeper = instrumentKeeper;
            this.clientKeeper = clientKeeper;
        }

        @Override
        public void init(ProcessorContext<Long, String> context) {
            this.context = context;
            executionReportStore = context.getStateStore(EXECUTION_REPORTS_STORE);
            tigerAllocationsStore = context.getStateStore(TIGER_ALLOCATIONS_STORE);
            executionAllocationRefsStore = context.getStateStore(EXECUTION_ALLOCATION_REF_STORE);
        }

        @Override
        public void process(Record<Long, UsStreetExecution> record) {
            UsStreetExecution usStreetExecution = record.value();
            executionReportStore.put(record.key(), usStreetExecution);
            logger.info(
                    "Stored execution. Execution id: {}, execution: {}",
                    record.key(),
                    usStreetExecution);
            switch (usStreetExecution.state) {
                case "NEW" -> {
                    logger.info("New execution. Execution id: {}", usStreetExecution.executionId);
                    TigerAllocation tigerAllocation = createTigerAllocation(usStreetExecution);
                    tigerAllocationsStore.put(tigerAllocation.id, tigerAllocation);
                    executionAllocationRefsStore.put(
                            usStreetExecution.executionId, tigerAllocation.id);
                    logger.info("New allocation. Allocation id: {}", tigerAllocation.id);
                    context.forward(
                            record.withValue(convertToCSV(tigerAllocation)), TIGER_ALLOCATION_SINK);
                }
                case "CANCEL" -> {
                    logger.info(
                            "Cancel execution. Execution id : {}",
                            usStreetExecution.executionId); // todo: should be ref_exec_id
                    long tigerAllocationId =
                            executionAllocationRefsStore.get(usStreetExecution.executionId);
                    if (tigerAllocationId > 0) {
                        TigerAllocation tigerAllocation =
                                tigerAllocationsStore.get(tigerAllocationId);
                        try {
                            tigerAllocation.state =
                                    tigerAllocation.state.transferTo(
                                            TigerAllocation.State.CANCEL_PENDING);
                            tigerAllocation.version++;
                            logger.info("Cancel allocation. Allocation id: {}", tigerAllocation.id);
                            context.forward(
                                    record.withValue(convertToCSV(tigerAllocation)),
                                    TIGER_ALLOCATION_SINK);
                        } catch (TigerAllocation.State.WrongStateTransitionException e) {
                            logger.error("Wrong state change request", e);
                        }
                    } else {
                        logger.warn(
                                "Could not find tiger allocation by execution id {}",
                                usStreetExecution.executionId);
                    }
                }
                default -> throw new RuntimeException("Wrong state " + usStreetExecution.state);
            }
        }

        private TigerAllocation createTigerAllocation(UsStreetExecution usStreetExecution) {
            TigerAllocation tigerAllocation = new TigerAllocation();
            tigerAllocation.id = generateNextId();
            tigerAllocation.state = TigerAllocation.State.NEW_PENDING;
            tigerAllocation.executionId = usStreetExecution.executionId;
            // todo reflect remaining fields from FbTigerAllocation in TigerAllocation class
            fillFromExecution(tigerAllocation, usStreetExecution);
            enrichInstrumentData(tigerAllocation, usStreetExecution);
            enrichClientData(tigerAllocation, usStreetExecution);
            return tigerAllocation;
        }

        /**
         * Generate id with a partition information stored in sequence. Which will be used later for
         * stream copartition
         *
         * @return new unique sequence
         */
        private long generateNextId() {
            int partition =
                    context.recordMetadata()
                            // Shouldn't be thrown in fact from `process` call
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Empty stream record metadata, unable to get"
                                                            + " partition for Sequencer"))
                            .partition();

            return new Sequencer(System::nanoTime, Namespace.US_STREET_CASH_EQUITY, partition)
                    .getNext();
        }

        private void enrichClientData(
                TigerAllocation tigerAllocation, UsStreetExecution execution) {
            ClientKeeper.Book book = clientKeeper.getBookById(execution.bookId);
            // todo: fill allocation
        }

        private void enrichInstrumentData(
                TigerAllocation tigerAllocation, UsStreetExecution execution) {
            InstrumentKeeper.Equity equity = instrumentKeeper.getById(execution.instrumentId);
            // todo: fill allocation
        }

        private void fillFromExecution(
                TigerAllocation tigerAllocation, UsStreetExecution execution) {
            // todo: copy data
            // tigerAllocation.securityId = execution.securityId;
        }
    }

    /**
     * Accepts parsed {@link TigerReply} and controls {@link TigerAllocation} state according to
     * {@link ReplyCode}
     */
    private static class TigerReplyProcessor
            implements Processor<Long, TigerReply, Object, Object> {

        private KeyValueStore<Long, TigerAllocation> tigerAllocationsStore;

        @Override
        public void init(ProcessorContext<Object, Object> context) {
            tigerAllocationsStore = context.getStateStore(TIGER_ALLOCATIONS_STORE);
        }

        @Override
        public void process(Record<Long, TigerReply> record) {
            TigerReply tigerReply = record.value();
            logger.info(
                    "Got reply from Tiger. Trade id:{}, state: {}",
                    tigerReply.allocationId,
                    tigerReply.replyCode.code);
            TigerAllocation tigerAllocation = tigerAllocationsStore.get(tigerReply.allocationId);

            if (tigerAllocation != null) {
                try {
                    TigerAllocation.State newState = TigerAllocation.State.NEW_PENDING;
                    // todo: check version, should be skipped? Check current behavior
                    // todo: check transaction type
                    // todo: generateNew state
                    // todo: set reason on nack
                    tigerAllocation.state.transferTo(newState);
                    tigerAllocation.version++;
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
     * Accepts raw reply in CSV format and converts it to allocationId, {@link TigerReply} records.
     * Key with allocationId is used in sink in custom partitioner.
     */
    public static class TigerReplyCoProcessor implements Processor<Long, String, Long, TigerReply> {

        private static final int COLUMNS_IN_REPLY = 3;
        private static final int CORRELATION_PARTS = 4;
        public static final String TIGER_TIMESTAMP_FORMAT = "yyyyMMdd-HH:mm:ss";
        private ProcessorContext<Long, TigerReply> context;

        @Override
        public void init(ProcessorContext<Long, TigerReply> context) {
            this.context = context;
        }

        @Override
        public void process(Record<Long, String> record) {
            logger.info("RCVD:{}", record.value());
            ParsedReply parsedReply = parseReply(record.value());

            if (US_STREET_PROCESSOR_APP_NAME.equals(parsedReply.applicationId)) {
                TigerReply tigerReply =
                        new TigerReply(
                                parsedReply.allocationId,
                                parsedReply.allocationVersion,
                                parsedReply.ackTimestamp,
                                parsedReply.replyCode,
                                parsedReply.replyTransactionType);
                context.forward(
                        new Record<>(
                                parsedReply.allocationId,
                                tigerReply,
                                record.timestamp(),
                                record.headers()),
                        TIGER_REPLY_SINK);
            } else {
                logger.info("SKIP app:{}", parsedReply.applicationId);
            }
        }

        /**
         * Converts reply raw CSV representation into {@link ParsedReply}. Might throw {@link
         * ParseException} if something goes wrong.
         *
         * @param csv raw tiger reply
         */
        private ParsedReply parseReply(String csv) {
            String[] columns = csv.split(",");
            if (columns.length != COLUMNS_IN_REPLY) {
                throw new ReplyParseException(
                        "Unexpected number of columns, must be " + COLUMNS_IN_REPLY);
            }

            String correlationId = columns[1];
            String[] correlationParts = correlationId.split("-");
            if (correlationParts.length != CORRELATION_PARTS) {
                throw new ReplyParseException(
                        "Unexpected number of correlation parts `"
                                + correlationId
                                + CORRELATION_PARTS);
            }

            try {
                ReplyCode replyCode = ReplyCode.getBy(columns[0]);

                ReplyTransactionType transactionType = ReplyTransactionType.getBy(Integer.parseInt(correlationParts[0]));
                String applicationId = correlationParts[1];
                long allocationId = Long.parseLong(correlationParts[2]);
                int allocationVersion = Integer.parseInt(correlationParts[3]);

                LocalDateTime ackTimestamp =
                        LocalDateTime.parse(
                                columns[2],
                                DateTimeFormatter.ofPattern(TIGER_TIMESTAMP_FORMAT, Locale.US));

                return new ParsedReply(
                        replyCode,
                        transactionType,
                        applicationId,
                        allocationId,
                        allocationVersion,
                        ackTimestamp);
            } catch (NumberFormatException e) {
                throw new ReplyParseException(e);
            }
        }

        /** Intermediate structure with all information available in reply */
        public static class ParsedReply {
            ReplyCode replyCode;
            ReplyTransactionType replyTransactionType;
            String applicationId;
            long allocationId;
            int allocationVersion;

            LocalDateTime ackTimestamp;

            public ParsedReply(
                    ReplyCode replyCode,
                    ReplyTransactionType replyTransactionType,
                    String applicationId,
                    long allocationId,
                    int allocationVersion,
                    LocalDateTime ackTimestamp) {
                this.replyCode = replyCode;
                this.replyTransactionType = replyTransactionType;
                this.applicationId = applicationId;
                this.allocationId = allocationId;
                this.allocationVersion = allocationVersion;
                this.ackTimestamp = ackTimestamp;
            }
        }

        /** Common exception for CSV parse process */
        public static class ReplyParseException extends RuntimeException {

            public ReplyParseException(String reason) {
                super(reason);
            }

            public ReplyParseException(Exception exception) {
                super(exception);
            }
        }
    }

    private static String convertToCSV(TigerAllocation tigerAllocation) {
        return "test,"
                + tigerAllocation.executionId
                + ","
                + tigerAllocation.id
                + ","
                + tigerAllocation.version
                + ","
                + tigerAllocation.state;
    }
}
