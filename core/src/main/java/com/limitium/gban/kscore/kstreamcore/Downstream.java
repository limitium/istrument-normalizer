package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.audit.AuditWrapperSupplier;
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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WrappedIndexedKeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperValue;
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
    WrappedKeyValueStore<Long, RequestData, Audit> requestDataOriginals;
    WrappedKeyValueStore<Long, RequestData, Audit> requestDataOverrides;
    WrappedIndexedKeyValueStore<String, Request, Audit> requests;

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
            WrappedKeyValueStore<Long, RequestData, Audit> requestDataOriginals,
            WrappedKeyValueStore<Long, RequestData, Audit> requestDataOverrides,
            WrappedIndexedKeyValueStore<String, Request, Audit> requests,
            Topic<? extends Kout, ? extends Vout> topic,
            boolean autoCommit
    ) {
        this.name = String.format("%s-%d", name, extendedProcessorContext.taskId().partition());
        this.extendedProcessorContext = extendedProcessorContext;
        this.requestDataOverrider = requestDataOverrider;
        this.requestConverter = converter;
        this.downstreamAmendModel = converter instanceof AmendConverter ? AMENDABLE : DownstreamAmendModel.CANCEL_NEW;
        this.correlationIdGenerator = correlationIdGenerator;
        this.requestDataOriginals = requestDataOriginals;
        this.requestDataOverrides = requestDataOverrides;
        this.requests = requests;
        this.topic = (Topic<Kout, Vout>) topic;
        this.autoCommit = autoCommit;
    }

    public void send(Request.RequestType requestType, long referenceId, int referenceVersion, RequestData requestData) {
        logger.debug("{} sends {} for {}:{} with {}", name, requestType, referenceId, referenceVersion, requestData);

        requestDataOriginals.putValue(referenceId, requestData);

        if (requestDataOverrides.get(referenceId) != null && requestDataOverrider == null) {
            logger.info("Skip fully overridden sends");
            return;
        }
        RequestContext<RequestData> requestContext = prepareRequestContext(requestType, referenceId, referenceVersion, requestData);

        processRequest(requestContext);
    }

    public void requestReplied(String correlationId, boolean isAck, @Nullable String code, @Nullable String answer, @Nullable String externalId, int externalVersion) {
        logger.debug("{} receive reply {} acked:{}", name, correlationId, isAck);
        WrapperValue<Audit, Request> auditRequest = requests.getUnique(DownstreamDefinition.STORE_REQUESTS_CORRELATION_INDEX_NAME, correlationId);
        if (auditRequest == null) {
            logger.error("NOT_FOUND:REQUEST:{}", correlationId);
            return;
        }

        Request request = auditRequest.value();
        if (isAck || request.state != Request.RequestState.ACKED) {
            request.state = isAck ? Request.RequestState.ACKED : Request.RequestState.NACKED;
            request.respondedCode = code;
            request.respondedMessage = answer;
            request.respondedAt = extendedProcessorContext.currentLocalTimeMs();
            request.externalId = externalId;
            request.externalVersion = externalVersion;
        }

        requests.putValue(request.getStoreKey(), request);
    }

    public void forceAckRequest(String correlationId) {
        logger.info("{} force ack for {}", name, correlationId);
        WrapperValue<Audit, Request> auditRequest = requests.getUnique(DownstreamDefinition.STORE_REQUESTS_CORRELATION_INDEX_NAME, correlationId);
        if (auditRequest == null) {
            logger.error("NOT_FOUND:REQUEST:{}", correlationId);
            return;
        }

        Request request = auditRequest.value();
        request.state = Request.RequestState.ACKED;
        request.respondedMessage = "force acked";
        request.respondedAt = extendedProcessorContext.currentLocalTimeMs();

        requests.putValue(request.getStoreKey(), request);
    }

    public void updateOverride(long referenceId, RequestData override) {
        logger.info("{} update override {} with {}", name, referenceId, override);
        if (override == null) {
            requestDataOverrides.delete(referenceId);
        } else {
            requestDataOverrides.putValue(referenceId, override);
        }
        resendRequest(referenceId);
    }

    public void resendRequest(long referenceId) {
        logger.info("{} resend last request for {}", name, referenceId);
        processRequest(
                restoreLastRequestContext(getLastRequest(referenceId)
                        .orElseThrow(() -> new RuntimeException("Unable to resend, noting was sent before")))
        );
    }

    public void retryRequest(long referenceId) {
        logger.info("{} retry last request for {}", name, referenceId);
        Request request = getLastRequest(referenceId)
                .orElseThrow(() -> new RuntimeException("Unable to resend, noting was sent before"));

        generateAndSendRequest(request.id,
                restoreLastRequestContext(request),
                createEffectiveRequest(request.type, request.effectiveReferenceId, request.effectiveVersion)
        );

        if (autoCommit) {
            requestReplied(request.correlationId, true, null, null, null, 0);
        }
    }

    public void terminateRequest(long referenceId, long requestId) {
        logger.info("{} terminate request for {}, id:{}", name, referenceId, requestId);
        Request request = getPreviousRequest(referenceId)
                .filter(r -> r.id == requestId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Request not found"));

        if (request.state != Request.RequestState.PENDING) {
            logger.info("{},{},{} non pending terminated", referenceId, requestId, request.state);
        }
        request.state = Request.RequestState.TERMINATED;
        request.respondedAt = extendedProcessorContext.currentLocalTimeMs();
        requests.putValue(request.getStoreKey(), request);
    }


    @NotNull
    private RequestContext<RequestData> restoreLastRequestContext(Request request) {
        RequestData requestData = requestDataOriginals.getValue(request.referenceId);
        return prepareRequestContext(request.type, request.referenceId, request.referenceVersion, requestData);
    }

    @NotNull
    private RequestContext<RequestData> prepareRequestContext(Request.RequestType requestType, long referenceId, int referenceVersion, RequestData requestData) {
        RequestData requestDataMerged = requestData;
        int overrideVersion = -1;
        WrapperValue<Audit, RequestData> auditRequestData = requestDataOverrides.get(referenceId);
        if (auditRequestData != null) {
            RequestData requestDataOverride = auditRequestData.value();
            overrideVersion = auditRequestData.wrapper().version();
            requestDataMerged = requestDataOverride;
            if (requestDataOverrider != null) {
                requestDataMerged = requestDataOverrider.override(requestData, requestDataOverride);
            }
        }

        logger.debug("{} merged override v:{}, result {}", name, overrideVersion, requestDataMerged);
        return new RequestContext<>(requestType, referenceId, referenceVersion, overrideVersion, requestDataMerged);
    }

    private void processRequest(RequestContext<RequestData> requestContext) {
        calculateEffectiveRequests(requestContext).forEach(effectiveRequest -> {
            Request request = generateAndSendRequest(generateNextId(), requestContext, effectiveRequest);

            logger.debug("{} sent request {}:{} {}:{},", name, request.type, request.correlationId, request.effectiveReferenceId, request.effectiveReferenceId);
            if (autoCommit) {
                requestReplied(request.correlationId, true, null, null, null, 0);
                logger.debug("{} autocommited request {}", name, request.correlationId);
            }
        });
    }

    private List<EffectiveRequest<RequestData, Kout, Vout>> calculateEffectiveRequests(RequestContext<RequestData> requestContext) {
        List<EffectiveRequest<RequestData, Kout, Vout>> effectiveRequest = new ArrayList<>();

        DownstreamReferenceState downstreamReferenceState = calculateDownstreamReferenceState(getLastNotNackedRequest(requestContext.referenceId));

        logger.debug("{} downstream state {} {}:{}", name, downstreamReferenceState.state, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion);

        switch (requestContext.requestType) {
            case NEW -> {
                switch (downstreamReferenceState.state) {
                    case UNAWARE -> effectiveRequest.add(createEffectiveRequest(NEW, requestContext.referenceId, 1));
                    case EXISTS -> {
                        switch (downstreamAmendModel) {
                            case AMENDABLE ->
                                    effectiveRequest.add(createEffectiveRequest(AMEND, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion + 1));
                            case CANCEL_NEW -> {
                                effectiveRequest.add(createEffectiveRequest(CANCEL, downstreamReferenceState.effectiveReferenceId, 1));
                                effectiveRequest.add(createEffectiveRequest(NEW, generateNextId(), 1));
                            }
                        }
                    }
                    case CANCELED ->
                            logger.info("{},{},{},{},{} Skip new on canceled", this.name, requestContext.requestType, requestContext.referenceId, requestContext.referenceVersion, downstreamReferenceState.state);
                }
            }
            case AMEND -> {
                switch (downstreamReferenceState.state) {
                    case UNAWARE -> effectiveRequest.add(createEffectiveRequest(NEW, requestContext.referenceId, 1));
                    case EXISTS -> {
                        switch (downstreamAmendModel) {
                            case AMENDABLE ->
                                    effectiveRequest.add(createEffectiveRequest(AMEND, downstreamReferenceState.effectiveReferenceId, downstreamReferenceState.effectiveReferenceVersion + 1));
                            case CANCEL_NEW -> {
                                effectiveRequest.add(createEffectiveRequest(CANCEL, downstreamReferenceState.effectiveReferenceId, 1));
                                effectiveRequest.add(createEffectiveRequest(NEW, generateNextId(), 1));
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
        KeyValueIterator<String, WrapperValue<Audit, Request>> prevRequests = requests.prefixScan(String.valueOf(referenceId), new StringSerializer());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(prevRequests, Spliterator.ORDERED), false)
                .onClose(prevRequests::close)
                .map((kv) -> kv.value.value())
                .sorted(Comparator.comparingLong((Request o) -> o.id).reversed());

    }

    private Optional<Request> getLastNotNackedRequest(long referenceId) {
        return getPreviousRequest(referenceId).filter(request -> request.state != Request.RequestState.NACKED).findFirst();
    }

    private Optional<Request> getLastRequest(long referenceId) {
        return getPreviousRequest(referenceId).findFirst();
    }


    private Request generateAndSendRequest(long requestId, RequestContext<RequestData> requestContext, EffectiveRequest<RequestData, Kout, Vout> effectiveRequest) {
        String correlationId = correlationIdGenerator.generate(requestId, effectiveRequest.requestType, requestContext.requestData, AuditWrapperSupplier.extractTraceId(extendedProcessorContext), extendedProcessorContext.getPartition());

        Request request = createRequest(effectiveRequest, requestId, correlationId, requestContext);

        requests.putValue(request.getStoreKey(), request);

        Record<Kout, Vout> record = effectiveRequest.createRecord(correlationId, requestContext.requestData);

        extendedProcessorContext.send(topic, record);
        return request;
    }

    private EffectiveRequest<RequestData, Kout, Vout> createEffectiveRequest(Request.RequestType requestType, long effectiveReferenceId, int effectiveVersion) {
        return new EffectiveRequest<>(requestType, switch (requestType) {
            case NEW -> requestConverter::newRequest;
            case AMEND -> ((AmendConverter<RequestData, Kout, Vout>) requestConverter)::amendRequest;
            case CANCEL -> requestConverter::cancelRequest;
            case SKIP -> throw new RuntimeException("Never called");
        }, effectiveReferenceId, effectiveVersion);
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
                requestContext.overrideVersion,
                extendedProcessorContext.currentLocalTimeMs());
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
