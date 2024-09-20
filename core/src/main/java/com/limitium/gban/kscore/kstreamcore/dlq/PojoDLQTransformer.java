package com.limitium.gban.kscore.kstreamcore.dlq;

import com.google.gson.Gson;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Transforms failed incoming message into DLQ record.
 *
 * @param <KIn> type of incoming record key
 * @param <VIn> type of incoming record value
 */
public class PojoDLQTransformer<KIn, VIn> implements DLQTransformer<KIn, VIn, DLQEnvelope> {
    private final Serde<VIn> pojoSerde;

    public PojoDLQTransformer(Serde<VIn> pojoSerde) {
        this.pojoSerde = pojoSerde;
    }

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
    @Override
    public Record<KIn, DLQEnvelope> transform(
            @Nonnull Record<KIn, VIn> failed,
            @Nonnull ExtendedProcessorContext<KIn, VIn, ?, ?> extendedProcessorContext,
            @Nullable String errorMessage,
            @Nullable Throwable exception) {

        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));

        String key = null;
        String subKey = null;
        if (exception instanceof DLQException dlqException) {
            key = dlqException.getKey();
            subKey = dlqException.getSubKey();

        }

        Gson gson = new Gson();
        String json = gson.toJson(failed.value());

        DLQEnvelope dlqEnvelope = new DLQEnvelope(
                extendedProcessorContext.getNextSequence(),
                errorMessage,
                sw.toString(),
                failed.value().getClass().getCanonicalName(),
                pojoSerde.serializer().serialize(extendedProcessorContext.getTopic(), failed.value()),
                json,
                extendedProcessorContext.getTopic(),
                extendedProcessorContext.getPartition(),
                extendedProcessorContext.getOffset(),
                exception.getClass().getCanonicalName(),
                key,
                subKey,
                extendedProcessorContext.currentLocalTimeMs()
        );

        return failed.withValue(dlqEnvelope);
    }
}
