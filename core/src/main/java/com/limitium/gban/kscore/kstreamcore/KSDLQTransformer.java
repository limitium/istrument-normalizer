package com.limitium.gban.kscore.kstreamcore;

import org.apache.kafka.streams.processor.api.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transforms failed incoming message into DLQ record.
 *
 * @param <KIn> type of incoming record key
 * @param <VIn> type of incoming record value
 * @param <DLQm> type of outgoing DLQ record value
 */
public interface KSDLQTransformer<KIn, VIn, DLQm> {

    /**
     * Transform failed incoming message into DLQ record.
     *
     * @param nextSequence next reserved sequence that can be used as id of new exception
     * @param failed       incoming message
     * @param fromTopic    message source topic
     * @param partition    incoming message partition
     * @param offset       incoming message offset
     * @param errorMessage human-readable explanation
     * @param exception    exception if occurred
     * @return new record for DLQ topic
     */
    @Nonnull
    Record<KIn, DLQm> transform(
            long nextSequence,
            @Nonnull Record<KIn, VIn> failed,
            @Nullable String fromTopic,
            int partition,
            long offset,
            @Nullable String errorMessage,
            @Nullable Throwable exception);
}
