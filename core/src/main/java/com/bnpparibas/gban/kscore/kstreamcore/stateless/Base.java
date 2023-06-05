package com.bnpparibas.gban.kscore.kstreamcore.stateless;

import com.bnpparibas.gban.kscore.kstreamcore.DLQ;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;

/**
 * Common definition for stateless process, must be used at least with {@link Converter} or {@link Partitioner}
 *
 * @param <KIn>
 * @param <VIn>
 * @param <KOut>
 * @param <VOut>
 * @param <DLQm>
 */
public interface Base<KIn, VIn, KOut, VOut, DLQm> {
    /**
     * Defines source topic for incoming messages
     *
     * @return
     */
    Topic<KIn, VIn> inputTopic();

    /**
     * Defines sink topic for outgoing messages
     * @return
     */
    Topic<KOut, VOut> outputTopic();

    /**
     * Defines dlq topic and transformer for failed convertations
     * @return
     */
    DLQ<KIn, VIn, DLQm> dlq();
}
