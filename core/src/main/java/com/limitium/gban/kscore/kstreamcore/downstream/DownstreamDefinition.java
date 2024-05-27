package com.limitium.gban.kscore.kstreamcore.downstream;

import com.limitium.gban.kscore.kstreamcore.KSTopology;
import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.CorrelationIdGenerator;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.NewCancelConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class DownstreamDefinition<RequestData, KOut, VOut> {
    public static String STORE_REQUEST_DATA_ORIGINALS_NAME = "request_data_originals";
    public static String STORE_REQUEST_DATA_OVERRIDES_NAME = "request_data_overrides";
    public static String STORE_REQUESTS_NAME = "requests";
    public static String STORE_REQUESTS_CORRELATION_INDEX_NAME = "CORRELATION_ID";


    public String name;
    public Serde<RequestData> requestDataSerde;
    public CorrelationIdGenerator<RequestData> correlationIdGenerator;
    public RequestDataOverrider<RequestData> requestDataOverrider;
    public NewCancelConverter<RequestData, KOut, VOut> requestConverter;
    public KSTopology.SinkDefinition<? extends KOut, ? extends VOut> sink;
    public ReplyDefinition<?, ?> replyDefinition;

    private Set<StoreBuilder<?>> builtStores;

    public DownstreamDefinition(@Nonnull String name, @Nonnull Serde<RequestData> requestDataSerde, @Nonnull CorrelationIdGenerator<RequestData> correlationIdGenerator, @Nonnull NewCancelConverter<RequestData, KOut, VOut> requestConverter, @Nonnull KSTopology.SinkDefinition<? extends KOut, ? extends VOut> sink, @Nullable ReplyDefinition<?, ?> replyDefinition, @Nullable RequestDataOverrider<RequestData> requestDataOverrider) {
        this.name = name;
        this.requestDataSerde = requestDataSerde;
        this.correlationIdGenerator = correlationIdGenerator;
        this.requestDataOverrider = requestDataOverrider;
        this.requestConverter = requestConverter;
        this.sink = sink;
        this.replyDefinition = replyDefinition;
    }

    public String getName() {
        return name;
    }

    public String getStoreName(String storeName) {
        return "downstream-" + name + "-" + storeName;
    }

    @SuppressWarnings("rawtypes")
    public Set<StoreBuilder<?>> buildOrGetStores() {
        if (builtStores == null) {
            builtStores = Set.of(
                    Stores2.auditableKeyValueStoreBuilder(Stores.persistentKeyValueStore(getStoreName(STORE_REQUEST_DATA_ORIGINALS_NAME)), Serdes.Long(), requestDataSerde),
                    Stores2.auditableKeyValueStoreBuilder(Stores.persistentKeyValueStore(getStoreName(STORE_REQUEST_DATA_OVERRIDES_NAME)), Serdes.Long(), requestDataSerde),
                    Stores2.auditableIndexedKeyValueStoreBuilder(Stores.persistentKeyValueStore(getStoreName(STORE_REQUESTS_NAME)), Serdes.String(), Request.RequestSerde())
                            .addUniqIndex(STORE_REQUESTS_CORRELATION_INDEX_NAME, auditRequest -> auditRequest.value().correlationId)
            );
        }
        return builtStores;
    }

    public record ReplyDefinition<KeyType, ReplyType>(KSTopology.SourceDefinition<KeyType, ReplyType> source,
                                                      DownstreamReplyProcessor.ReplyConsumer<KeyType, ReplyType> replyConsumer) {

    }
}
