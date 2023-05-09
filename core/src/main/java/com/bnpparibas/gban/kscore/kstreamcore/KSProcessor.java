package com.bnpparibas.gban.kscore.kstreamcore;

import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class KSProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private Sequencer sequencer;

    protected ProcessorContext<KOut, VOut> context;
    private Topic<KIn, ?> dlqTopic;
    private KSDLQTransformer<KIn, VIn, ?> dlqTransformer;

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        Processor.super.init(this.context);
        sequencer = new Sequencer(context::currentSystemTimeMs, getApplicationNamespace(), context.taskId().partition());
    }

    /**
     * Provide uniq application id like name or business part. Up to 2^{@link Sequencer#NAMESPACE_BITS}
     *
     * @return application id
     */
    protected int getApplicationNamespace() {
        return 0;
    }

    public <DLQm> void setDLQRecordGenerator(Topic<KIn, DLQm> topic, KSDLQTransformer<KIn, VIn, DLQm> dlqTransformer) {
        this.dlqTopic = topic;
        this.dlqTransformer = dlqTransformer;
    }

    /**
     * Generate next sequence based on stream time and partition. Might stuck if consumes more than 2^{@link Sequencer#SEQUENCE_BITS} messages in a single millisecond.
     *
     * @return a new sequence
     */
    protected long getNextSequence() {
        return sequencer.getNext();
    }

    /**
     * Sends message to sink topic
     *
     * @param topic sink topic
     * @param record message to send
     * @param <KOutl> topic key type
     * @param <VOutl> topic value type
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <KOutl, VOutl> void send(Topic<KOutl, VOutl> topic, Record<KOutl, VOutl> record) {
        context.forward((Record) record, KSTopology.TopologyNameGenerator.sinkName(topic));
    }

    protected void sendToDLQ(Record<KIn, VIn> failed, @Nullable String errorMessage) {
        sendToDLQ(failed, errorMessage, null);
    }

    protected void sendToDLQ(Record<KIn, VIn> failed, @Nullable Throwable exception) {
        sendToDLQ(failed, null, exception);
    }

    /**
     * Sends message to DLQ topic.
     *
     * @param failed incoming message
     * @param errorMessage additional explanation
     * @param exception if occurs
     * @see #sendToDLQ(Record, String)
     * @see #sendToDLQ(Record, Throwable)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void sendToDLQ(Record<KIn, VIn> failed, @Nullable String errorMessage, @Nullable Throwable exception) {
        if (this.dlqTopic == null || this.dlqTransformer == null) {
            throw new RuntimeException("DLQ wasn't setup properly. Use KSTopology.addProcessor().withDLQ() method to set.");
        }

        String fromTopic = context.recordMetadata().map(RecordMetadata::topic).orElse(null);
        long offset = context.recordMetadata().map(RecordMetadata::offset).orElse(-1L);
        int partition = context.recordMetadata().map(RecordMetadata::partition).orElse(-1);

        Record dlqRecord = dlqTransformer.transform(
                failed,
                fromTopic,
                partition,
                offset,
                errorMessage,
                exception);

        send(dlqTopic, dlqRecord);
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
    protected <KS, VS> IndexedKeyValueStore<KS, VS> getIndexedStore(String name) {
        Objects.requireNonNull(context, "Context is missed. Probably KSProcessor.super.init() call is absent");

        return ((WrappedStateStore<IndexedKeyValueStore<KS, VS>, KS, VS>) context.getStateStore(name)).wrapped();
    }
}
