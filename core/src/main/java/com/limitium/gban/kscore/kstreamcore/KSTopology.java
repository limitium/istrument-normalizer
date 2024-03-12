package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.downstream.*;
import com.limitium.gban.kscore.kstreamcore.processor.BaseProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessor;
import com.limitium.gban.kscore.kstreamcore.processor.ProcessorMeta;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.InjectableKeyValueStoreBuilder;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.limitium.gban.kscore.kstreamcore.KSTopology.TopologyNameGenerator.*;

/**
 * Composite around {@link Topology} to reduce complexity of topology description
 */
public class KSTopology {
    final Topology topology;
    private final KafkaStreamsConfiguration config;//@todo: hash app.name to sequencer.namespace
    final Set<ProcessorDefinition<?, ?, ?, ?>> processors = new HashSet<>();

    public KSTopology(Topology topology, KafkaStreamsConfiguration config) {
        this.topology = topology;
        this.config = config;
    }

    /**
     * Processor spawner for topology implementation. One processor per source partition. If stream application runs with multiple threads, then concurrent effects might occur.
     *
     * @param <kI>
     * @param <vI>
     * @param <kO>
     * @param <vO>
     */
    public interface ExtendedProcessorSupplier<kI, vI, kO, vO> {
        ExtendedProcessor<kI, vI, kO, vO> get();
    }

    /**
     * Extended source definition
     *
     * @param topic
     * @param isPattern
     * @param <K>
     * @param <V>
     */
    public record SourceDefinition<K, V>(@Nonnull Topic<K, V> topic, boolean isPattern) {
    }

    /**
     * Extended sink definition
     */
    public static class SinkDefinition<K, V> {
        @Nonnull
        private final Topic<K, V> topic;
        @Nullable
        private final TopicNameExtractor<K, V> topicNameExtractor;
        @Nullable
        private final StreamPartitioner<K, V> streamPartitioner;

        /**
         * @param topic
         * @param topicNameExtractor
         * @param streamPartitioner
         * @param <K>
         * @param <V>
         */
        public SinkDefinition(@Nonnull Topic<K, V> topic,
                              @Nullable TopicNameExtractor<K, V> topicNameExtractor,
                              @Nullable StreamPartitioner<K, V> streamPartitioner) {
            this.topic = topic;
            this.topicNameExtractor = topicNameExtractor;
            this.streamPartitioner = streamPartitioner;
        }

        @Nonnull
        public Topic<K, V> topic() {
            return topic;
        }

        @Nullable
        public TopicNameExtractor<K, V> topicNameExtractor() {
            return topicNameExtractor;
        }

        @Nullable
        public StreamPartitioner<K, V> streamPartitioner() {
            return streamPartitioner;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (SinkDefinition) obj;
            return Objects.equals(this.topic, that.topic) &&
                    Objects.equals(this.topicNameExtractor, that.topicNameExtractor) &&
                    Objects.equals(this.streamPartitioner, that.streamPartitioner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, topicNameExtractor, streamPartitioner);
        }

        @Override
        public String toString() {
            return "SinkDefinition[" +
                    "topic=" + topic + ", " +
                    "topicNameExtractor=" + topicNameExtractor + ", " +
                    "streamPartitioner=" + streamPartitioner + ']';
        }

    }

    /**
     * Describes stream topology starting from processor
     *
     * @param <kI>
     * @param <vI>
     * @param <kO>
     * @param <vO>
     */
    public static class ProcessorDefinition<kI, vI, kO, vO> {
        public static class CachedProcessorSupplier<kI, vI, kO, vO> implements ProcessorSupplier<kI, vI, kO, vO> {
            private final AtomicReference<ExtendedProcessor<kI, vI, kO, vO>> cachedProcessor = new AtomicReference<>();
            private final ExtendedProcessorSupplier<kI, vI, kO, vO> processorSupplier;
            private final ProcessorMeta<kI, vI, kO, vO> processorMeta;

            public CachedProcessorSupplier(ExtendedProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
                this.processorMeta = new ProcessorMeta<>();
                this.processorSupplier = processorSupplier;

                ExtendedProcessor<kI, vI, kO, vO> firstCachedProcessor = processorSupplier.get();
                this.cachedProcessor.set(firstCachedProcessor);

                @SuppressWarnings("rawtypes")
                Class<? extends ExtendedProcessor> extendedProcessorClass = firstCachedProcessor.getClass();
                setCustomName(extendedProcessorClass.getName().replace(extendedProcessorClass.getPackageName() + ".", ""));
            }

            @Override
            public BaseProcessor<kI, vI, kO, vO> get() {
                ExtendedProcessor<kI, vI, kO, vO> processor = Optional.ofNullable(cachedProcessor.getAndSet(null))
                        .orElse(processorSupplier.get());

                return new BaseProcessor<>(processor, processorMeta);
            }

            String getProcessorSimpleClassName() {
                return processorMeta.name;
            }

            public <DLQm> void setDLQ(Topic<kI, DLQm> dlq, DLQTransformer<kI, vI, ? super DLQm> dlqTransformer) {
                processorMeta.dlqTopic = dlq;
                processorMeta.dlqTransformer = dlqTransformer;
            }

            public void addDownstream(DownstreamDefinition<?, kO, vO> downstreamDefinition) {
                processorMeta.downstreamDefinitions.put(downstreamDefinition.name, downstreamDefinition);
            }

            public void setCustomName(String name) {
                processorMeta.name = name;
            }
        }

        private final KSTopology ksTopology;
        private final CachedProcessorSupplier<kI, vI, kO, vO> processorSupplier;
        private final Set<SourceDefinition<kI, ? extends vI>> sources = new HashSet<>();
        private final Set<SinkDefinition<? extends kO, ? extends vO>> sinks = new HashSet<>();
        private Set<DownstreamDefinition<?, ? extends kO, ? extends vO>> downstreams = new HashSet<>();
        private final Set<StoreBuilder<?>> stores = new HashSet<>();

        public ProcessorDefinition(KSTopology ksTopology, ExtendedProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
            this.ksTopology = ksTopology;
            this.processorSupplier = new CachedProcessorSupplier<>(processorSupplier);
        }

        /**
         * Connects processor with the source topic. Can be extended with {@link SourceDefinition}
         *
         * @param topic source topic for processor
         * @return processor builder
         * @see #withSource(SourceDefinition)
         */
        public ProcessorDefinition<kI, vI, kO, vO> withSource(Topic<kI, ? extends vI> topic) {
            return withSource(new SourceDefinition<>(topic, false));
        }

        public ProcessorDefinition<kI, vI, kO, vO> withSource(SourceDefinition<kI, ? extends vI> source) {
            sources.add(source);
            return this;
        }

        /**
         * Connects processor with the store
         *
         * @param stores
         * @return
         */
        public ProcessorDefinition<kI, vI, kO, vO> withStores(StoreBuilder<?>... stores) {
            Set<StoreBuilder<?>> builderSet = new HashSet<>();
            for (StoreBuilder<?> storeBuilder : stores) {
                if (storeBuilder instanceof IndexedKeyValueStoreBuilder<?, ?> idxBuilder) {
                    builderSet.addAll(idxBuilder.getIndexBuilders());
                }
                builderSet.add(storeBuilder);
            }
            this.stores.addAll(builderSet);
            this.processorSupplier.processorMeta.storeNames.addAll(builderSet.stream().map(StoreBuilder::name).collect(Collectors.toSet()));
            return this;
        }

        /**
         * Connects processor with the sink topic. Can be extended with {@link SinkDefinition}
         *
         * @param topic sink topic for processor
         * @return processor builder
         * @see #withSink(SinkDefinition)
         */
        public ProcessorDefinition<kI, vI, kO, vO> withSink(Topic<? extends kO, ? extends vO> topic) {
            return withSink(new SinkDefinition<>(topic, null, null));
        }

        public ProcessorDefinition<kI, vI, kO, vO> withSink(SinkDefinition<? extends kO, ? extends vO> sink) {
            sinks.add(sink);
            return this;
        }

        @SuppressWarnings("unchecked")
        public ProcessorDefinition<kI, vI, kO, vO> withBroadcast(Broadcast<? extends vO> broadcast) {
            sinks.add((SinkDefinition<? extends kO, ? extends vO>) broadcast);
            return this;
        }

        /**
         * Adds DQL to processor.
         *
         * @param dlq            topic
         * @param dlqTransformer transforms failed income message into a DLQ record
         * @param <DLQm>         dlq topic value type
         * @return processor builder
         * @see #withDLQ(DLQ)
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <DLQm> ProcessorDefinition<kI, vI, kO, vO> withDLQ(Topic<kI, DLQm> dlq, DLQTransformer<kI, vI, ? super DLQm> dlqTransformer) {
            this.processorSupplier.setDLQ(dlq, dlqTransformer);
            return withSink((Topic) dlq);
        }

        public <DLQm> ProcessorDefinition<kI, vI, kO, vO> withDLQ(DLQ<kI, vI, DLQm> dlq) {
            return withDLQ(dlq.topic, dlq.transformer);
        }

        /**
         * Add downstream to processor
         *
         * @param downstreamDefinition
         * @param <RequestData>        internal downstream data model
         * @return
         */
        public <RequestData> ProcessorDefinition<kI, vI, kO, vO> withDownstream(DownstreamDefinition<RequestData, kO, vO> downstreamDefinition) {
            this.downstreams.add(downstreamDefinition);
            this.sinks.add(downstreamDefinition.sink);
            this.processorSupplier.addDownstream(downstreamDefinition);
            return this;
        }

        /**
         * Override default processor naming strategy with a custom name
         *
         * @param name name to override
         * @return processor builder
         */
        private ProcessorDefinition<kI, vI, kO, vO> withCustomName(String name) {
            this.processorSupplier.setCustomName(name);
            return this;
        }

        public KSTopology done() {
            ksTopology.createProcessor(this);
            return ksTopology;
        }
    }

    private <kO, vI, vO, kI> void createProcessor(ProcessorDefinition<kI, vI, kO, vO> processorDefinition) {
        processors.add(processorDefinition);
    }

    /**
     * Starts processor builder. Limitation - no processor chaining
     *
     * @param processorSupplier processor instance supplier
     * @param <kI>              processor source key type
     * @param <vI>              processor source value type
     * @param <kO>              processor sink key type
     * @param <vO>              processor sink value type
     * @return processor builder
     * @see ProcessorDefinition#withSource(Topic)
     * @see ProcessorDefinition#withStores(StoreBuilder[])
     * @see ProcessorDefinition#withSink(Topic)
     * @see ProcessorDefinition#withDLQ(Topic, DLQTransformer)
     */
    public <kI, vI, kO, vO> ProcessorDefinition<kI, vI, kO, vO> addProcessor(ExtendedProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
        return new ProcessorDefinition<>(this, processorSupplier);
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull ExtendedProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source) {
        return addProcessor(processorSupplier, source, null, new StoreBuilder[]{});
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull ExtendedProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source,
            @Nonnull StoreBuilder<?>... stores) {
        return addProcessor(processorSupplier, source, null, stores);
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull ExtendedProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source,
            @Nonnull Topic<kOl, vOl> sink) {
        return addProcessor(processorSupplier, source, sink, new StoreBuilder[]{});
    }

    /**
     * Creates processor with a single source, single sink and stores in one shot.
     *
     * @param processorSupplier
     * @param source
     * @param sink
     * @param stores
     * @param <kIl>
     * @param <vIl>
     * @param <kOl>
     * @param <vOl>
     * @return
     * @see #addProcessor(ExtendedProcessorSupplier, Topic)
     * @see #addProcessor(ExtendedProcessorSupplier, Topic, Topic)
     * @see #addProcessor(ExtendedProcessorSupplier, Topic, StoreBuilder[])
     * @see #addProcessor(ExtendedProcessorSupplier, Topic, Topic, StoreBuilder[])
     */
    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull ExtendedProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source,
            @Nullable Topic<kOl, vOl> sink,
            @Nullable StoreBuilder<?>... stores) {

        ProcessorDefinition<kIl, vIl, kOl, vOl> processorDefinition = addProcessor(processorSupplier)
                .withSource(source);

        if (sink != null) {
            processorDefinition.withSink(sink);
        }
        if (stores != null && stores.length > 0) {
            processorDefinition.withStores(stores);
        }
        return processorDefinition.done();
    }

    /**
     * Shouldn't be called directly.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void buildTopology() {
        Set<SourceDefinition<?, ?>> sources = new HashSet<>();

        Map<SinkDefinition<?, ?>, Set<ProcessorDefinition<?, ?, ?, ?>>> sinksForProcessors = new HashMap<>();
        Map<StoreBuilder<?>, Set<ProcessorDefinition<?, ?, ?, ?>>> storeForProcessors = new HashMap<>();

        String topicPrefix = config.asProperties().getProperty(KStreamConfig.MANAGED_TOPIC_PREFIX);

        //Collect all downstream from all mentions
        Set<DownstreamDefinition<?, ?, ?>> downstreamDefinitions = processors.stream()
                .flatMap((processorDefinition) -> processorDefinition.downstreams.stream())
                .collect(Collectors.toSet());

        //Define axillary processors around downstream
        downstreamDefinitions.forEach(downstreamDefinition -> {
            if (downstreamDefinition.replyDefinition != null) {
                createProcessor(new ProcessorDefinition(this, () -> new DownstreamReplyProcessor(downstreamDefinition.name, downstreamDefinition.replyDefinition.replyConsumer()))
                        .withCustomName("DownstreamReplyProcessor-" + downstreamDefinition.name)
                        .withSource(downstreamDefinition.replyDefinition.source())
                        .withDownstream(downstreamDefinition));
            }
            createProcessor(new ProcessorDefinition(this, () -> new DownstreamOverrideProcessor(downstreamDefinition.name))
                    .withCustomName("DownstreamOverrideProcessor-" + downstreamDefinition.name)
                    .withSource(new Topic(topicPrefix + ".downstream-" + downstreamDefinition.name + "-override", Serdes.Long(), downstreamDefinition.requestDataSerde))
                    .withDownstream(downstreamDefinition));
            createProcessor(new ProcessorDefinition(this, () -> new DownstreamResendProcessor(downstreamDefinition.name))
                    .withCustomName("DownstreamResendProcessor-" + downstreamDefinition.name)
                    .withSource(new Topic(topicPrefix + ".downstream-" + downstreamDefinition.name + "-resend", Serdes.Long(), Serdes.String()))
                    .withDownstream(downstreamDefinition));
            createProcessor(new ProcessorDefinition(this, () -> new DownstreamTerminateProcessor(downstreamDefinition.name))
                    .withCustomName("DownstreamTerminateProcessor-" + downstreamDefinition.name)
                    .withSource(new Topic(topicPrefix + ".downstream-" + downstreamDefinition.name + "-terminate", Serdes.Long(), Serdes.Long()))
                    .withDownstream(downstreamDefinition));
            createProcessor(new ProcessorDefinition(this, () -> new DownstreamForceAckProcessor(downstreamDefinition.name))
                    .withCustomName("DownstreamForceAckProcessor-" + downstreamDefinition.name)
                    .withSource(new Topic(topicPrefix + ".downstream-" + downstreamDefinition.name + "-forceack", Serdes.String(), Serdes.String()))
                    .withDownstream(downstreamDefinition));
        });


        //Connect dowstreams stores to related processors
        processors.stream().
                filter(processorDefinition -> !processorDefinition.downstreams.isEmpty())
                .forEach(processorDefinition -> {
                    StoreBuilder[] downstreamsStores = processorDefinition.downstreams
                            .stream()
                            .flatMap(downstreamDefinition -> downstreamDefinition.buildOrGetStores().stream())
                            .toArray(StoreBuilder[]::new);
                    processorDefinition.withStores(downstreamsStores);
                });


        //add injectors
        processors.stream()
                .flatMap(processorDefinition -> processorDefinition.stores.stream())
                .collect(Collectors.toSet())
                .forEach(store -> {
                    if (store instanceof InjectableKeyValueStoreBuilder<?, ?, ?> injectable) {
                        if (injectable.isInjectable()) {
                            String injectTopic = topicPrefix + ".store-" + store.name() + "-inject";
                            Topic topic = new Topic(injectTopic, getStoreSerde(store, "keySerde"), getStoreSerde(store, "valueSerde"));

                            createProcessor(new ProcessorDefinition(this, () -> new KSInjectProcessor(store))
                                    .withCustomName("KSInjectProcessor-" + store.name())
                                    .withSource(topic)
                                    .withStores(store));
                        }
                    }
                });

        processors.forEach((processor -> {
            processor.sources.forEach(source -> {
                if (!sources.contains(source)) {
                    if (source.isPattern) {
                        topology.addSource(
                                sourceName(source.topic),
                                source.topic.keySerde.deserializer(),
                                source.topic.valueSerde.deserializer(),
                                Pattern.compile(source.topic.topic)
                        );
                    } else {
                        topology.addSource(
                                sourceName(source.topic),
                                source.topic.keySerde.deserializer(),
                                source.topic.valueSerde.deserializer(),
                                source.topic.topic
                        );
                    }
                    sources.add(source);
                }
            });

            topology.addProcessor(
                    processorName(processor.processorSupplier),
                    processor.processorSupplier,
                    processor.sources.stream()
                            .map((source -> TopologyNameGenerator.sourceName(source.topic)))
                            .toArray(String[]::new)
            );

            processor.sinks.forEach(sink -> {
                sinksForProcessors.putIfAbsent(sink, new HashSet<>());
                sinksForProcessors.get(sink).add(processor);
            });

            processor.stores.forEach((store) -> {
                storeForProcessors.putIfAbsent(store, new HashSet<>());
                storeForProcessors.get(store).add(processor);
            });
        }));

        sinksForProcessors.forEach((sink, processors) -> {
            String[] processorNames = processors.stream()
                    .map(processor -> processorName(processor.processorSupplier))
                    .toArray(String[]::new);

            if (sink.streamPartitioner == null && sink.topicNameExtractor == null) {
                topology.addSink(
                        sinkName(sink.topic),
                        sink.topic.topic,
                        sink.topic.keySerde.serializer(),
                        sink.topic.valueSerde.serializer(),
                        processorNames);
            } else if (sink.streamPartitioner != null && sink.topicNameExtractor != null) {
                topology.addSink(
                        sinkName(sink.topic),
                        (TopicNameExtractor) sink.topicNameExtractor,
                        sink.topic.keySerde.serializer(),
                        sink.topic.valueSerde.serializer(),
                        (StreamPartitioner) sink.streamPartitioner,
                        processorNames);
            } else if (sink.streamPartitioner != null) {
                topology.addSink(
                        sinkName(sink.topic),
                        sink.topic.topic,
                        sink.topic.keySerde.serializer(),
                        sink.topic.valueSerde.serializer(),
                        (StreamPartitioner) sink.streamPartitioner,
                        processorNames);
            } else if (sink.topicNameExtractor != null) {
                topology.addSink(
                        sinkName(sink.topic),
                        (TopicNameExtractor) sink.topicNameExtractor,
                        sink.topic.keySerde.serializer(),
                        sink.topic.valueSerde.serializer(),
                        processorNames);
            }
        });

        storeForProcessors.forEach((store, processors) -> {
            String[] processorNames = processors.stream()
                    .map(processor -> processorName(processor.processorSupplier))
                    .toArray(String[]::new);

            topology.addStateStore(store, processorNames);
        });
    }

    @SuppressWarnings("rawtypes")
    private Serde getStoreSerde(StoreBuilder<?> store, String serdeFieldName) {
        try {
            Field serdeField = AbstractStoreBuilder.class.getDeclaredField(serdeFieldName);
            serdeField.setAccessible(true);
            return (Serde) serdeField.get(store);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TopologyNameGenerator {
        @Nonnull
        public static String processorName(ProcessorDefinition.CachedProcessorSupplier<?, ?, ?, ?> supplier) {
            return processorName(supplier.getProcessorSimpleClassName());
        }

        @Nonnull
        public static String processorName(String processorName) {
            return "prc__" + processorName;
        }

        @Nonnull
        public static String sinkName(Topic<?, ?> topic) {
            return "dst__" + topic.topic;
        }

        @Nonnull
        public static String sourceName(Topic<?, ?> topic) {
            return sourceName(topic.topic);
        }

        @Nonnull
        public static String sourceName(String topic) {
            return "src__" + topic;
        }
    }
}
