package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transforms failed incoming message into DLQ record.
 *
 * @param <KIn>  type of incoming record key
 * @param <VIn>  type of incoming record value
 * @param <DLQm> type of outgoing DLQ record value
 */
public interface DLQTransformer<KIn, VIn, DLQm> {

    /**
     * Transform failed incoming message into DLQ record.
     *
     * @param failed                   incoming message
     * @param extendedProcessorContext context bound to failed record
     * @param errorMessage             human-readable explanation
     * @param exception                exception if occurred
     * @return new record for DLQ topic
     */
    @Nonnull
    Record<KIn, DLQm> transform(
            @Nonnull Record<KIn, VIn> failed,
            @Nonnull ExtendedProcessorContext<KIn, VIn, ?, ?> extendedProcessorContext,
            @Nullable String errorMessage,
            @Nullable Throwable exception);
}
