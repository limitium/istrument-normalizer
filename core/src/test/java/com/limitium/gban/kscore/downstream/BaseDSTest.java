package com.limitium.gban.kscore.downstream;

import com.limitium.gban.bibliotheca.sequencer.Sequencer;
import com.limitium.gban.kscore.KStreamApplication;
import com.limitium.gban.kscore.kstreamcore.Downstream;
import com.limitium.gban.kscore.kstreamcore.KSTopology;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.limitium.gban.kscore.kstreamcore.Topic;
import com.limitium.gban.kscore.kstreamcore.audit.Audit;
import com.limitium.gban.kscore.kstreamcore.downstream.DownstreamDefinition;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.AmendConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.CorrelationIdGenerator;
import com.limitium.gban.kscore.kstreamcore.downstream.converter.NewCancelConverter;
import com.limitium.gban.kscore.kstreamcore.downstream.state.Request;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import com.limitium.gban.kscore.test.BaseKStreamApplicationTests;
import com.limitium.gban.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.WrapperValue;
import org.apache.kafka.streams.state.internals.WrapperValueSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;

@KafkaTest(
        topics = {
                "tpc.in.1",
                "reply.ds1",
                "reply.ds3",
                "core-app.downstream-ds1-override",
                "core-app.downstream-ds1-resend",
                "core-app.downstream-ds1-cancel",
                "core-app.downstream-ds2-override",
                "core-app.downstream-ds2-resend",
                "core-app.downstream-ds2-cancel",
                "core-app.downstream-ds3-override",
                "core-app.downstream-ds3-resend",
                "core-app.downstream-ds3-cancel",
        },
        consumers = {
                "ds.out.1",
                "ds.out.2",
                "ds.out.3",
                "core-app.store-downstream-ds1-request_data_originals-changelog",
                "core-app.store-downstream-ds1-request_data_overrides-changelog",
                "core-app.store-downstream-ds1-requests-changelog",
                "core-app.store-downstream-ds2-request_data_originals-changelog",
                "core-app.store-downstream-ds2-request_data_overrides-changelog",
                "core-app.store-downstream-ds2-requests-changelog",
                "core-app.store-downstream-ds3-request_data_originals-changelog",
                "core-app.store-downstream-ds3-request_data_overrides-changelog",
                "core-app.store-downstream-ds3-requests-changelog",
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                BaseDSTest.TopologyConfig.class
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class BaseDSTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, Long> SOURCE = new Topic<>("tpc.in.1", Serdes.Integer(), Serdes.Long());

    public static final Topic<String, String> REPLY1 = new Topic<>("reply.ds1", Serdes.String(), Serdes.String());
    public static final Topic<Long, String> OVERRIDE1 = new Topic<>("core-app.downstream-ds1-override", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> RESEND1 = new Topic<>("core-app.downstream-ds1-resend", Serdes.Long(), Serdes.String());
    public static final Topic<Long, Long> CANCEL1 = new Topic<>("core-app.downstream-ds1-cancel", Serdes.Long(), Serdes.Long());
    public static final Topic<String, String> SINK1 = new Topic<>("ds.out.1", Serdes.String(), Serdes.String());
    public static final Topic<String, WrapperValue<Audit, Request>> REQUESTS1 = new Topic<>("core-app.store-downstream-ds1-requests-changelog", Serdes.String(), WrapperValueSerde.create(Audit.AuditSerde(), Request.RequestSerde()));

    public static final Topic<Long, String> OVERRIDE2 = new Topic<>("core-app.downstream-ds2-override", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> RESEND2 = new Topic<>("core-app.downstream-ds2-resend", Serdes.Long(), Serdes.String());
    public static final Topic<Long, Long> CANCEL2 = new Topic<>("core-app.downstream-ds2-cancel", Serdes.Long(), Serdes.Long());
    public static final Topic<String, String> SINK2 = new Topic<>("ds.out.2", Serdes.String(), Serdes.String());
    public static final Topic<String, WrapperValue<Audit, Request>> REQUESTS2 = new Topic<>("core-app.store-downstream-ds2-requests-changelog", Serdes.String(), WrapperValueSerde.create(Audit.AuditSerde(), Request.RequestSerde()));
    public static final Topic<String, String> REPLY3 = new Topic<>("reply.ds3", Serdes.String(), Serdes.String());

    public static final Topic<Long, String> OVERRIDE3 = new Topic<>("core-app.downstream-ds3-override", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> RESEND3 = new Topic<>("core-app.downstream-ds3-resend", Serdes.Long(), Serdes.String());
    public static final Topic<Long, Long> CANCEL3 = new Topic<>("core-app.downstream-ds3-cancel", Serdes.Long(), Serdes.Long());
    public static final Topic<String, String> SINK3 = new Topic<>("ds.out.3", Serdes.String(), Serdes.String());
    public static final Topic<String, WrapperValue<Audit, Request>> REQUESTS3 = new Topic<>("core-app.store-downstream-ds3-requests-changelog", Serdes.String(), WrapperValueSerde.create(Audit.AuditSerde(), Request.RequestSerde()));

    public static class TopologyConfig {
        public static class TestProcessor implements ExtendedProcessor<Integer, Long, String, String> {
            private Downstream<String, String, String> ds1;
            private Downstream<String, String, String> ds2;
            private Downstream<String, String, String> ds3;

            private KeyValueStore<Integer, String> inMemKv;

            @Override
            public void init(ExtendedProcessorContext<Integer, Long, String, String> context) {
                inMemKv = context.getStateStore("in_mem_kv");
                ds1 = context.getDownstream("ds1");
                ds2 = context.getDownstream("ds2");
                ds3 = context.getDownstream("ds3");
            }

            @Override
            public void process(Record<Integer, Long> record) {
                Request.RequestType requestType;
                long referenceId = record.key();
                int referenceVersion = 1;
                String requestData = String.join("|", String.valueOf(record.value()), String.valueOf(referenceVersion));

                String prefVal = inMemKv.get(record.key());
                if (prefVal == null) {
                    requestType = Request.RequestType.NEW;
                    inMemKv.put(record.key(), requestData);
                } else {
                    String[] parts = prefVal.split("\\|");
                    referenceVersion = Integer.parseInt(parts[1]);
                    if (record.value() == 0) {
                        requestType = Request.RequestType.CANCEL;
                        requestData = null;
                        inMemKv.delete(record.key());
                    } else {
                        requestType = Request.RequestType.AMEND;
                        referenceVersion++;
                        requestData = String.join("|", String.valueOf(record.value()), String.valueOf(referenceVersion));
                        inMemKv.put(record.key(), requestData);
                    }
                }

                ds1.send(requestType, referenceId, referenceVersion, "rd1>" + requestData);
                ds2.send(requestType, referenceId, referenceVersion, "rd2>" + requestData);
                ds3.send(requestType, referenceId, referenceVersion, "rd3>" + requestData);
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            DownstreamDefinition<String, String, String> ds1 = new DownstreamDefinition<>("ds1", Serdes.String(), new CorrelationIdGenerator<>() {
                @Override
                public String generate(long requestId, String rd) {
                    return String.valueOf(requestId);
                }
            }, (toOverride, override) -> toOverride + "+" + override, new NewCancelConverter<>() {
                @Override
                public Record<String, String> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "new", "ds1", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), rd), System.currentTimeMillis());
                }

                @Override
                public Record<String, String> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "cancel", "ds1", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), "-"), System.currentTimeMillis());
                }
            },
                    new KSTopology.SinkDefinition<>(SINK1, null, (topic, key, value, numPartitions) -> Sequencer.getPartition(Long.parseLong(key))),
                    new DownstreamDefinition.ReplyDefinition<>(new KSTopology.SourceDefinition<>(new Topic<>("reply.ds1", Serdes.String(), Serdes.String()), false), (record, requestsAware) -> {
                        String[] parts = record.value().split(",");
                        requestsAware.replied(record.key(), Boolean.parseBoolean(parts[0]), parts[1], parts[2]);
                    }));

            DownstreamDefinition<String, String, String> ds2 = new DownstreamDefinition<>("ds2", Serdes.String(), new CorrelationIdGenerator<>() {
                @Override
                public String generate(long requestId, String rd) {
                    return String.valueOf(requestId);
                }
            }, (toOverride, override) -> toOverride + "+" + override, new AmendConverter<>() {
                @Override
                public Record<String, String> amendRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "amend", "ds2", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), rd), System.currentTimeMillis());
                }

                @Override
                public Record<String, String> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "new", "ds2", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), rd), System.currentTimeMillis());
                }

                @Override
                public Record<String, String> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "cancel", "ds2", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), "-"), System.currentTimeMillis());
                }
            },
                    new KSTopology.SinkDefinition<>(SINK2, null, (topic, key, value, numPartitions) -> Sequencer.getPartition(Long.parseLong(key))),
                    null);


            DownstreamDefinition<String, String, String> ds3 = new DownstreamDefinition<>("ds3", Serdes.String(), new CorrelationIdGenerator<>() {
                @Override
                public String generate(long requestId, String rd) {
                    return String.valueOf(requestId);
                }
            }, (toOverride, override) -> toOverride + "+" + override, new AmendConverter<>() {
                @Override
                public Record<String, String> amendRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "amend", "ds3", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), rd), System.currentTimeMillis());
                }

                @Override
                public Record<String, String> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "new", "ds3", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), rd), System.currentTimeMillis());
                }

                @Override
                public Record<String, String> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, String rd) {
                    return new Record<>(correlationId, String.join(",", "cancel", "ds3", String.valueOf(effectiveReferenceId), String.valueOf(effectiveReferenceVersion), "-"), System.currentTimeMillis());
                }
            },
                    new KSTopology.SinkDefinition<>(SINK3, null, (topic, key, value, numPartitions) -> Sequencer.getPartition(Long.parseLong(key))),
                    new DownstreamDefinition.ReplyDefinition<>(new KSTopology.SourceDefinition<>(new Topic<>("reply.ds3", Serdes.String(), Serdes.String()), false), (record, requestsAware) -> {
                        String[] parts = record.value().split(",");
                        requestsAware.replied(record.key(), Boolean.parseBoolean(parts[0]), parts[1], parts[2]);
                    }));


            return builder -> {
                StoreBuilder<KeyValueStore<Integer, String>> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("in_mem_kv"), Serdes.Integer(), Serdes.String());

                builder
                        .addProcessor(TestProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .withDownstream(ds1)
                        .withDownstream(ds2)
                        .withDownstream(ds3)
                        .done();
            };

        }
    }

    public Outgoing parseOutput(ConsumerRecord<String, String> record) {
        String[] parts = record.value().split(",");

        return new Outgoing(
                record.key(),
                parts[0],
                parts[1],
                parts[2],
                parts[3],
                parts[4]
        );
    }

    public record Outgoing(String correlationId, String requestType, String dsId, String refId, String refVer,
                           String payload) {
    }

    ;
}
