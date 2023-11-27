package com.limitium.gban.kscore.kstreamcore.processor;

import com.limitium.gban.bibliotheca.sequencer.Sequencer;
import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.KSTopology;
import com.limitium.gban.kscore.kstreamcore.Topic;
import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.downstream.DownstreamDefinition;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedIndexedKeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.ProcessorPostInitListener;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.streams.state.internals.WrapperSupplierFactoryAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class ExtendedProcessorContext<KIn, VIn, KOut, VOut> extends ProcessorContextComposer<KOut, VOut> {
    Logger logger = LoggerFactory.getLogger(getClass());
    public static final String SEQUENCER_NAMESPACE = "sequencer.namespace";
    private final Sequencer sequencer;
    private final ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta;
    private Record<KIn, VIn> incomingRecord;


    public ExtendedProcessorContext(ProcessorContext<KOut, VOut> context, ProcessorMeta<KIn, VIn, KOut, VOut> processorMeta) {
        super(context);
        this.processorMeta = processorMeta;
        sequencer = new Sequencer(context::currentSystemTimeMs, (Integer) context.appConfigs().getOrDefault(SEQUENCER_NAMESPACE, 0), context.taskId().partition());
    }

    Map<String, StateStore> stores = new HashMap<>();

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <S extends StateStore> S getStateStore(String name) {
        return (S) stores.computeIfAbsent(name, s -> {
            S stateStore = ExtendedProcessorContext.this.context.getStateStore(name);

            if (stateStore instanceof WrappedStateStore wrappedStateStore) {
                if (wrappedStateStore.wrapped() instanceof WrapperSupplierFactoryAware wrapperSupplierFactoryAware) {
                    if (wrapperSupplierFactoryAware instanceof IndexedMeteredKeyValueStore indexedKeyValueStore) {
                        return new WrappedIndexedKeyValueDecorator(indexedKeyValueStore, wrapperSupplierFactoryAware, this);
                    }
                    if (wrapperSupplierFactoryAware instanceof KeyValueStore kvStore) {
                        return new WrappedKeyValueDecorator(kvStore, wrapperSupplierFactoryAware, this);
                    }
                }
                if (wrappedStateStore.wrapped() instanceof WrappedKeyValueStore wrappedKeyValueStore) {
                    return wrappedKeyValueStore;
                }
                if (wrappedStateStore.wrapped() instanceof IndexedKeyValueStore indexedKeyValueStore) {
                    return indexedKeyValueStore;
                }
            }
            return stateStore;
        });
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

    public boolean hasDLQ() {
        return processorMeta.dlqTopic != null && processorMeta.dlqTransformer != null;
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

        WrappedKeyValueStore<Long, RequestData, Audit> requestDataOriginals = getStateStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUEST_DATA_ORIGINALS_NAME));
        WrappedKeyValueStore<Long, RequestData, Audit> requestDataOverrides = getStateStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUEST_DATA_OVERRIDES_NAME));
        WrappedIndexedKeyValueStore<String, Request, Audit> requests = getStateStore(downstreamDefinition.getStoreName(DownstreamDefinition.STORE_REQUESTS_NAME));

        Downstream<RequestData, KOut, VOut> downstream = new Downstream<>(
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
        logger.info("ExtendedContext {}-{} creates `{}` {}, with request store {}", this, taskId().partition(), downstreamDefinition.name, downstream, requests);
        return downstream;
    }

    /**
     * Return the topic name of the current input record; could be {@code ""} if it is not
     * available.
     * <p> For example, if this method is invoked within a @link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated topic.
     *
     * @return the topic name
     */
    public String getTopic() {
        return recordMetadata().map(RecordMetadata::topic).orElse("");
    }

    /**
     * Return the partition id of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a @link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated partition id.
     *
     * @return the offset
     */
    public long getOffset() {
        return recordMetadata().map(RecordMetadata::offset).orElse(-1L);
    }

    /**
     * Return the offset of the current input record; could be -1 if it is not available.
     *
     * <p> For example, if this method is invoked within a @link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated offset.
     *
     * @return the partition id
     */
    public int getPartition() {
        return recordMetadata().map(RecordMetadata::partition).orElse(-1);
    }

    protected void updateIncomingRecord(Record<KIn, VIn> incomingRecord) {
        this.incomingRecord = incomingRecord;
    }

    @SuppressWarnings("unchecked")
    protected void postProcessorInit() {
        logger.info("PostInit extendedContext {}, post init stores", this);
        processorMeta.storeNames.stream()
                .map(this::getStateStore)
                .filter(ProcessorPostInitListener.class::isInstance)
                .map(ProcessorPostInitListener.class::cast)
                .peek(s -> logger.info("PostInit store {}", s))
                .forEach(listener -> listener.onPostInit(this));
    }
}
