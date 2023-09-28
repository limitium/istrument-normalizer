package com.limitium.gban.kscore.kstreamcore.processor;

import com.limitium.gban.bibliotheca.sequencer.Sequencer;
import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.KSTopology;
import com.limitium.gban.kscore.kstreamcore.Topic;
import com.limitium.gban.kscore.kstreamcore.downstream.DownstreamDefinition;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class ExtendedProcessorContext<KIn, VIn, KOut, VOut> extends ProcessorContextCompositor<KOut, VOut> {
    public static final String SEQUENCER_NAMESPACE = "sequencer.namespace";
    public static final Function<Headers, Long> TRACE_ID_EXTRACTOR = headers -> Optional.of(headers.lastHeader("traceparent"))
            .map(header -> new String(header.value(), Charset.defaultCharset()))
            .filter(Strings::isNotEmpty)
            .map(v -> v.split("-"))
            .filter(parts -> parts.length > 1)
            .map(parts -> Long.parseLong(parts[1]))
            .orElse(-1L);

    private final Sequencer sequencer;
    private final ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta;
    private Record<KIn, VIn> incomingRecord;

    @SuppressWarnings("rawtypes")
    private final Set<IndexedKeyValueStore> indexedStores = new HashSet<>();


    public ExtendedProcessorContext(ProcessorContext<KOut, VOut> context, ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta) {
        super(context);
        this.processorMeta = processorMeta;
        sequencer = new Sequencer(context::currentSystemTimeMs, (Integer) context.appConfigs().getOrDefault(SEQUENCER_NAMESPACE, 0), context.taskId().partition());
    }

    /**
     * Unwraps IndexedKeyValueStore from context
     *
     * @param name registered store name
     * @param <KS> key type
     * @param <VS> value type
     * @return store
     */
    @SuppressWarnings("unchecked")
    public <KS, VS> IndexedKeyValueStore<KS, VS> getIndexedStore(String name) {
        IndexedKeyValueStore<KS, VS> indexedKeyValueStore = ((WrappedStateStore<IndexedKeyValueStore<KS, VS>, KS, VS>) context.getStateStore(name)).wrapped();
        indexedStores.add(indexedKeyValueStore);
        return indexedKeyValueStore;
    }

    /**
     * Generate next sequence based on stream time and partition. Might stuck if consumes more than 2^{@link Sequencer#SEQUENCE_BITS} messages in a single millisecond.
     *
     * @return a new sequence
     */
    public long getNextSequence() {
        return sequencer.getNext();
    }

    public Headers getIncomingRecordHeaders() {
        return incomingRecord.headers();
    }

    public long getIncomingRecordTimestamp() {
        return incomingRecord.timestamp();
    }

    public long getTraceId() {
        return TRACE_ID_EXTRACTOR.apply(getIncomingRecordHeaders());
    }

    /**
     * Sends message to sink topic
     *
     * @param topic   sink topic
     * @param record  message to send
     * @param <KOutl> topic key type
     * @param <VOutl> topic value type
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <KOutl, VOutl> void send(Topic<KOutl, VOutl> topic, Record<KOutl, VOutl> record) {
        forward((Record) record, KSTopology.TopologyNameGenerator.sinkName(topic));
    }

    public void sendToDLQ(Record<KIn, VIn> failed, Exception exception) {
        sendToDLQ(failed, exception.getMessage(), exception);
    }

    /**
     * Sends message to DLQ topic.
     *
     * @param failed       incoming message
     * @param errorMessage additional explanation
     * @param exception    if occurs
     * @see #sendToDLQ(Record, Exception)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void sendToDLQ(Record<KIn, VIn> failed, @Nullable String errorMessage, @Nullable Exception exception) {
        if (processorMeta.dlqTopic == null || processorMeta.dlqTransformer == null) {
            throw new RuntimeException("DLQ wasn't setup properly. Use KSTopology.addProcessor().withDLQ() method to set.");
        }
        Record dlqRecord = processorMeta.dlqTransformer.transform(
                failed,
                this,
                errorMessage,
                exception);

        send(processorMeta.dlqTopic, dlqRecord);
    }

    @SuppressWarnings("unchecked")
    public <RequestData> Downstream<RequestData, KOut, VOut> getDownstream(String name) {
        DownstreamDefinition<RequestData, KOut, VOut> downstreamDefinition = (DownstreamDefinition<RequestData, KOut, VOut>) processorMeta.downstreamDefinitions.get(name);
        if (downstreamDefinition == null) {
            throw new RuntimeException("Unable to find downstream with name:" + name + ", in " + this);
        }

        KeyValueStore<Long, RequestData> requestDataOriginals = context.getStateStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUEST_DATA_ORIGINALS_NAME));
        KeyValueStore<Long, RequestData> requestDataOverrides = context.getStateStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUEST_DATA_OVERRIDES_NAME));
        IndexedKeyValueStore<String, Request> requests = getIndexedStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUESTS_NAME));

        return new Downstream<>(
                name,
                this,
                downstreamDefinition.requestDataOverrider,
                downstreamDefinition.requestConverter,
                downstreamDefinition.correlationIdGenerator,
                requestDataOriginals,
                requestDataOverrides,
                requests,
                downstreamDefinition.sink.topic(),
                downstreamDefinition.replyDefinition == null
        );
    }

    protected void updateIncomingRecord(Record<KIn, VIn> incomingRecord) {
        this.incomingRecord = incomingRecord;
    }

    protected void postProcessorInit() {
        indexedStores.forEach(IndexedKeyValueStore::rebuildIndexes);
    }
}
