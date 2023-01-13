package com.bnpparibas.gban.kscore.kstreamcore;

import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.Objects;

public abstract class KSProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private Sequencer sequencer;
    protected ProcessorContext<KOut, VOut> context;
    private String traceIdHeader = "traceparent";
    private Serde<Long> traceIdDeserializer = Serdes.Long();
    private Serializer<?> dlqValueSerializer = Serdes.String().serializer();

    private Topic<?, ?> dlqTopic;

    @Override
    public void init(ProcessorContext<KOut, VOut> context) {
        this.context = context;
        Processor.super.init(this.context);
        sequencer = new Sequencer(context::currentStreamTimeMs, getApplicationNamespace(), context.taskId().partition());
    }

    /**
     * Provide uniq application id like name or business part. Up to 2^{@link Sequencer#NAMESPACE_BITS}
     *
     * @return
     */
    protected int getApplicationNamespace() {
        return 0;
    }

    /**
     * Generate next sequence based on stream time and partition. Might stuck if consumes more than 2^{@link Sequencer#SEQUENCE_BITS} messages in a single millisecond.
     *
     * @return
     */
    protected long getNextSequence() {
        return sequencer.getNext();
    }

    /**
     * Get traceId from record header
     *
     * @return
     */
    protected long getTraceId(Record<KIn, VIn> record) {
        return traceIdDeserializer.deserializer().deserialize(null, record.headers().lastHeader(traceIdHeader).value());
    }

    protected void send(Topic<KOut, VOut> topic, Record<KOut, VOut> record) {
        context.forward(record, KSTopology.TopologyNameGenerator.sinkName(topic));
    }

    @SuppressWarnings("unchecked")
    protected void toDLQ(Record<KIn, VIn> record, String message) {
        Record<KIn, Object> dlqRecord = record.withValue(message);
        //todo: wrap message and original value with common message
        ((ProcessorContext<KIn, Object>) context).forward(dlqRecord);
    }

    /**
     * Unwraps IndexedKeyValueStore from context
     *
     * @param name registered store name
     * @param <KS> key type
     * @param <VS> value type
     * @return
     */
    @SuppressWarnings("unchecked")
    protected <KS, VS> IndexedKeyValueStore<KS, VS> getIndexedStore(String name) {
        Objects.requireNonNull(context, "Context is missed. Probably KSProcessor.super.init() call is absent");

        return ((WrappedStateStore<IndexedKeyValueStore<KS, VS>, KS, VS>) context.getStateStore(name)).wrapped();
    }
}
