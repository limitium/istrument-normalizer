package com.limitium.gban.kscore.kstreamcore.audit;

import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier;
import org.apache.logging.log4j.util.Strings;

import java.nio.charset.Charset;
import java.util.Optional;

import static com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier.AuditHeaders.*;

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
        long createdAt = context.currentSystemTimeMs();
        if (audit != null) {
            version = audit.version();
            createdAt = audit.createdAt();
        }
        String modifiedBy = Optional.ofNullable(context.getIncomingRecordHeaders().lastHeader(USER  ))
                .map(header -> new String(header.value(), Charset.defaultCharset()))
                .orElse(null);

        String reason = Optional.ofNullable(context.getIncomingRecordHeaders().lastHeader(REASON))
                .map(header -> new String(header.value(), Charset.defaultCharset()))
                .orElse(null);

        long traceId = Optional.ofNullable(context.getIncomingRecordHeaders().lastHeader(TRACE))
                .map(header -> new String(header.value(), Charset.defaultCharset()))
                .filter(Strings::isNotEmpty)
                .map(v -> v.split("-"))
                .filter(parts -> parts.length > 1)
                .map(parts -> Long.parseLong(parts[1]))
                .orElse(-1L);

        return new Audit(
                traceId,
                ++version,
                context.getPartition(),
                createdAt,
                context.currentSystemTimeMs(),
                modifiedBy,
                reason,
                value == null
        );
    }
}