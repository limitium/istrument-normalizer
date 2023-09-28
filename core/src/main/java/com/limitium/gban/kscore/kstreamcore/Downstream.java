package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.downstream.DownstreamDefinition;
import com.limitium.gban.kscore.kstreamcore.downstream.RequestContext;
import com.limitium.gban.kscore.kstreamcore.downstream.RequestDataOverrider;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.AmendConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.CorrelationIdGenerator;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.NewCancelConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.state.DownstreamReferenceState;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.limitium.gban.kscore.kstreamcore.Downstream.DownstreamAmendModel.AMENDABLE;
import static com.limitium.gban.kscore.kstreamcore.downstream.state.Request.RequestType.*;


public class Downstream<RequestData, Kout, Vout> {
    Logger logger = LoggerFactory.getLogger(Downstream.class);
    String name;

    //Upstream processor instance

    ExtendedProcessorContext<?, ?, Kout, Vout> extendedProcessorContext;

    //Converters
    RequestDataOverrider<RequestData> requestDataOverrider;
    NewCancelConverter<RequestData, Kout, Vout> requestConverter;
    CorrelationIdGenerator<RequestData> correlationIdGenerator;

    //State
    KeyValueStore<Long, RequestData> requestDataOriginal;
    KeyValueStore<Long, RequestData> requestDataOverrides; //todo: must be wrapped with audit store
    IndexedKeyValueStore<String, Request> requests; //todo: must be wrapped with audit store

    //Downstream
    DownstreamAmendModel downstreamAmendModel;
    Topic<Kout, Vout> topic;
    private final boolean autoCommit;

    @SuppressWarnings("unchecked")
    public Downstream(
            String name,
            ExtendedProcessorContext<?, ?, Kout, Vout> extendedProcessorContext,
            RequestDataOverrider<RequestData> requestDataOverrider,
            NewCancelConverter<RequestData, Kout, Vout> converter,
            CorrelationIdGenerator<RequestData> correlationIdGenerator,
            KeyValueStore<Long, RequestData> requestDataOriginal,
            KeyValueStore<Long, RequestData> requestDataOverrides,
            IndexedKeyValueStore<String, Request> requests,
            Topic<? extends Kout, ? extends Vout> topic,
            boolean autoCommit
    ) {
        this.name = name;
        this.extendedProcessorContext = extendedProcessorContext;
        this.requestDataOverrider = requestDataOverrider;
        this.requestConverter = converter;
        this.downstreamAmendModel = converter instanceof AmendConverter ? AMENDABLE : DownstreamAmendModel.CANCEL_NEW;
        this.correlationIdGenerator = correlationIdGenerator;
        this.requestDataOriginal = requestDataOriginal;
        this.requestDataOverrides = requestDataOverrides;
        this.requests = requests;
        this.topic = (Topic<Kout, Vout>) topic;
        this.autoCommit = autoCommit;
    }

    public void send(Request.RequestType requestType, long referenceId, int referenceVersion, RequestData requestData) {
        RequestContext<RequestData> requestContext = prepareRequestContext(requestType, referenceId, referenceVersion, requestData);

        requestDataOriginal.put(referenceId, requestData);
        processRequest(requestContext);
    }

    public void replied(String correlationId, boolean isAck, @Nullable String code, @Nullable String answer, @Nullable String externalId, int externalVersion) {
        Request request = requests.getUnique(DownstreamDefinition.STORE_REQUESTS_CORRELATION_INDEX_NAME, correlationId);
        if (request == null) {
            throw new RuntimeException("NF");
        }

        if (request.state == Request.RequestState.CANCELED) {
            throw new RuntimeException("CANCELED");
        }

        request.state = isAck ? Request.RequestState.ACKED : Request.RequestState.NACKED;
        request.respondedCode = code;
        request.respondedMessage = answer;
        request.respondedAt = System.currentTimeMillis();
        request.externalId = externalId;
        request.externalVersion = externalVersion;

        requests.put(request.getStoreKey(), request);
    }

    public void updateOverride(long referenceId, RequestData override) {
        //todo: bump override version, after auditable store
        requestDataOverrides.put(referenceId, override);
        resend(referenceId);
    }

    public void resend(long referenceId) {
        Request request = getLastRequest(referenceId)
                .orElseThrow(() -> new RuntimeException("Unable to resend, noting was sent before"));

        RequestData requestData = requestDataOriginal.get(referenceId);

        RequestContext<RequestData> requestContext = prepareRequestContext(request.type, request.referenceId, request.referenceVersion, requestData);
        processRequest(requestContext);
    }

    public void cancel(long referenceId, long id) {
        Request request = getPreviousRequest(referenceId)
                .filter(r -> r.id == id)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Request not found"));

        if (request.state != Request.RequestState.PENDING) {
            logger.info("{},{},{} non pending canceled", referenceId, id, request.state);
        }
        request.state = Request.RequestState.CANCELED;
        request.respondedAt = System.currentTimeMillis();
        requests.put(request.getStoreKey(), request);
    }

    @NotNull
    private RequestContext<RequestData> prepareRequestContext(Request.RequestType requestType, long referenceId, int referenceVersion, RequestData requestData) {
        RequestData requestDataMerged = requestData;
        int overrideVersion = -1;
        RequestData requestDataOverride = requestDataOverrides.get(referenceId);
        if (requestDataOverride != null) {
            overrideVersion = 1;//auditable version
            requestDataMerged = requestDataOverrider.override(requestData, requestDataOverride);
        }

        return new RequestContext<>(requestType, referenceId, referenceVersion, overrideVersion, requestDataMerged);
    }

    private void processRequest(RequestContext<RequestData> requestContext) {
        calculateEffectiveRequests(requestContext).forEach(effectiveRequest -> {
            Request request = generateAndSendRequest(requestContext, effectiveRequest);
            if (autoCommit) {
                replied(request.correlationId, true, null, null, null, 0);
            }
        });
    }

    private List<EffectiveRequest<RequestData, Kout, Vout>> calculateEffectiveRequests(RequestContext<RequestData> requestContext) {
        List<EffectiveRequest<RequestData, Kout, Vout>> effectiveRequest = new ArrayList<>();

        DownstreamReferenceState downstreamReferenceState = calculateDownstreamReferenceState(getLastNotNackedRequest(requestContext.referenceId));

        switch (requestContext.requestType) {
            case NEW -> {
                switch (downstreamReferenceState.state) {
                    case UNAWARE ->
                            effectiveRequest.add(new EffectiveRequest<>(NEW, requestConverter::newRequest, requestContext.referenceId, 1));
                    case EXISTS -> {
                        switch (downstreamAmendModel) {
                            case AMENDABLE ->
                                    effectiveRequest.add(new EffectiveRequest<>(AMEND, ((AmendConverter<RequestData, Kout, Vout>) requestConverter)::amendRequest, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion + 1));
                            case CANCEL_NEW -> {
                                effectiveRequest.add(new EffectiveRequest<>(CANCEL, requestConverter::cancelRequest, downstreamReferenceState.effectiveReferenceId, 1));
                                effectiveRequest.add(new EffectiveRequest<>(NEW, requestConverter::newRequest, generateNextId(), 1));
                            }
                        }
                    }
                    case CANCELED ->
                            logger.info("{},{},{},{},{} Skip new on canceled", this.name, requestContext.requestType, requestContext.referenceId, requestContext.referenceVersion, downstreamReferenceState.state);
                }
            }
            case AMEND -> {
                switch (downstreamReferenceState.state) {
                    case UNAWARE ->
                            effectiveRequest.add(new EffectiveRequest<>(NEW, requestConverter::newRequest, requestContext.referenceId, 1));
                    case EXISTS -> {
                        switch (downstreamAmendModel) {
                            case AMENDABLE ->
                                    effectiveRequest.add(new EffectiveRequest<>(AMEND, ((AmendConverter<RequestData, Kout, Vout>) requestConverter)::amendRequest, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion + 1));
                            case CANCEL_NEW -> {
                                effectiveRequest.add(new EffectiveRequest<>(CANCEL, requestConverter::cancelRequest, downstreamReferenceState.effectiveReferenceId, 1));
                                effectiveRequest.add(new EffectiveRequest<>(NEW, requestConverter::newRequest, generateNextId(), 1));
                            }
                        }
                    }
                    case CANCELED ->
                            logger.info("{},{},{},{},{} Skip new on canceled", this.name, requestContext.requestType, requestContext.referenceId, requestContext.referenceVersion, downstreamReferenceState.state);
                }
            }
            case CANCEL -> {
                switch (downstreamReferenceState.state) {
                    case UNAWARE ->
                            logger.info("{},{},{},{},{} Skip unaware cancel", this.name, requestContext.requestType, requestContext.referenceId, requestContext.referenceVersion, downstreamReferenceState.state);
                    case EXISTS ->
                            effectiveRequest.add(new EffectiveRequest<>(CANCEL, requestConverter::cancelRequest, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion));
                    case CANCELED ->
                            logger.info("{},{},{},{},{} Skip double cancel", this.name, requestContext.requestType, requestContext.referenceId, requestContext.referenceVersion, downstreamReferenceState.state);
                }
            }
            case SKIP -> {
                logger.info("Skip downstream");
            }
        }
        return effectiveRequest;
    }

    /**
     * Reversed stream of requests, from last to first.
     *
     * @param referenceId
     * @return
     */
    @NotNull
    private Stream<Request> getPreviousRequest(long referenceId) {
        KeyValueIterator<String, Request> prevRequests = requests.prefixScan(String.valueOf(referenceId), new StringSerializer());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(prevRequests, Spliterator.ORDERED), false)
                .map((kv) -> kv.value)
                .sorted(Comparator.comparingLong((Request o) -> o.id).reversed());
    }

    private Optional<Request> getLastNotNackedRequest(long referenceId) {
        return getPreviousRequest(referenceId).filter(request -> request.state != Request.RequestState.NACKED).findFirst();
    }

    private Optional<Request> getLastRequest(long referenceId) {
        return getPreviousRequest(referenceId).findFirst();
    }


    private Request generateAndSendRequest(RequestContext<RequestData> requestContext, EffectiveRequest<RequestData, Kout, Vout> effectiveRequest) {
        long requestId = generateNextId();
        String correlationId = correlationIdGenerator.generate(requestId, requestContext.requestData);

        Request request = createRequest(effectiveRequest, requestId, correlationId, requestContext);

        requests.put(request.getStoreKey(), request);

        //todo: headers are lost, restore them!
        Record<Kout, Vout> record = effectiveRequest.createRecord(correlationId, requestContext.requestData);

        extendedProcessorContext.send(topic, record);
        return request;
    }

    private Request createRequest(EffectiveRequest<RequestData, Kout, Vout> effectiveRequest, long requestId, String correlationId, RequestContext<RequestData> requestContext) {
        return new Request(
                requestId,
                correlationId,
                effectiveRequest.requestType,
                effectiveRequest.referenceId,
                effectiveRequest.referenceVersion,
                requestContext.referenceId,
                requestContext.referenceVersion,
                requestContext.overrideVersion);
    }

    private long generateNextId() {
        return extendedProcessorContext.getNextSequence();
    }

    @NotNull
    private DownstreamReferenceState calculateDownstreamReferenceState(Optional<Request> lastRequest) {
        return lastRequest
                .map(r -> new DownstreamReferenceState(r.type == CANCEL ? DownstreamReferenceState.ReferenceState.CANCELED : DownstreamReferenceState.ReferenceState.EXISTS, r.effectiveReferenceId, r.effectiveVersion))
                .orElse(new DownstreamReferenceState(DownstreamReferenceState.ReferenceState.UNAWARE, -1, -1));
    }


    interface EffectiveRequestConverter<RequestData, Kout, Vout> {
        Record<Kout, Vout> convert(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, RequestData requestData);
    }

    record EffectiveRequest<RequestData, Kout, Vout>(Request.RequestType requestType,
                                                     EffectiveRequestConverter<RequestData, Kout, Vout> requestConverter,
                                                     long referenceId, int referenceVersion) {

        public Record<Kout, Vout> createRecord(String correlationId, RequestData requestData) {
            return requestConverter.convert(correlationId, referenceId, referenceVersion, requestData);
        }
    }

    enum DownstreamAmendModel {
        CANCEL_NEW, AMENDABLE
    }
}
