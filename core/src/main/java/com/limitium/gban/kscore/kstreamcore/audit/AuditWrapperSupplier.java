package com.limitium.gban.kscore.kstreamcore.audit;

import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;

import static com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier.AuditHeaders.REASON;
import static com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier.AuditHeaders.USER;

@SuppressWarnings("rawtypes")
public class AuditWrapperSupplier<K, V> extends WrapperSupplier<K, V, Audit, ExtendedProcessorContext> {

    public static class AuditHeaders {
        public static final String TRACE = "traceparent";
        public static final String PREFIX = "ksc_";
        public static final String REASON = PREFIX + "reason";
        public static final String USER = PREFIX + "user";
    }

    public AuditWrapperSupplier(WrappedKeyValueStore<K, V, Audit> store, ExtendedProcessorContext context) {
        super(store, context);
    }

    @Override
    public Audit generate(K key, V value) {
        Audit audit = store.getWrapper(key);
        int version = 0;
        long createdAt = context.currentLocalTimeMs();
        if (audit != null) {
            version = audit.version();
            createdAt = audit.createdAt();
        }
        String modifiedBy = getStrFromHeader(context.getIncomingRecordHeaders(), USER)
                .orElse(null);

        String reason = getStrFromHeader(context.getIncomingRecordHeaders(), REASON)
                .orElse(null);

        long traceId = extractTraceId(context);

        return new Audit(
                traceId,
                ++version,
                context.getPartition(),
                createdAt,
                context.currentLocalTimeMs(),
                modifiedBy,
                reason,
                value == null
        );
    }

    @NotNull
    public static Long extractTraceId(ExtendedProcessorContext context) {
        return getStrFromHeader(context.getIncomingRecordHeaders(), AuditHeaders.TRACE)
                .map(v -> v.split("-"))
                .filter(parts -> parts.length > 1)
                .map(parts -> Long.parseLong(parts[1]))
                .orElse(-1L);
    }

    @NotNull
    private static Optional<String> getStrFromHeader(Headers incomingRecordHeaders, String header) {
        return Optional.ofNullable(incomingRecordHeaders.lastHeader(header))
                .map(Header::value)
                .filter(Objects::nonNull)
                .map(value -> new String(value, Charset.defaultCharset()))
                .filter(Strings::isNotEmpty);
    }
}