package com.bnpparibas.gban.kscore.kstreamcore;

import com.bnpparibas.gban.bibliotheca.sequencer.Sequencer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.Objects;

public abstract class KSProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {

    private Sequencer sequencer;
    protected ProcessorContext<KOut, VOut> context;

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
    protected abstract int getApplicationNamespace();

    /**
     * Generate next sequence based on stream time and partition. Might stuck if consumes more than 2^{@link Sequencer#SEQUENCE_BITS} messages in a single millisecond.
     *
     * @return
     */
    protected long getNextSequence() {
        return sequencer.getNext();
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
