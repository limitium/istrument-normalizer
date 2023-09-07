package com.limitium.gban.kscore.kstreamcore.stateless;

/**
 * Defines custom partitioner for outgoing Record<KOut, VOut> to {@link Base#outputTopic()}
 *
 * @param <KIn>
 * @param <VIn>
 * @param <KOut>
 * @param <VOut>
 * @param <DLQm>
 */
public interface Partitioner<KIn, VIn, KOut, VOut, DLQm> extends Base<KIn, VIn, KOut, VOut, DLQm> {
    /**
     * Repartionate {@link Base#outputTopic()} with the similar to {@link org.apache.kafka.streams.processor.StreamPartitioner#partition(String, Object, Object, int)} signature,
     * based on outgoing Record<KOut, VOut> key and value
     *
     * @param topic
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    int partition(String topic, KOut key, VOut value, int numPartitions);
}
