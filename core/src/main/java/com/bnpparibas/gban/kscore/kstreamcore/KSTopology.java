package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.bnpparibas.gban.kscore.kstreamcore.KSTopology.TopologyNameGenerator.*;

/**
 * Composite around {@link Topology} to reduce complexity of topology description
 */
public class KSTopology {
    final Topology topology;
    final Set<ProcessorDefinition<?, ?, ?, ?>> processors = new HashSet<>();

    public KSTopology(Topology topology) {
        this.topology = topology;
    }

    /**
     * Processor spawner for topology implementation. One processor per source partition. If stream application runs with multiple threads, then concurrent effects might occur.
     *
     * @param <kI>
     * @param <vI>
     * @param <kO>
     * @param <vO>
     */
    public interface KSProcessorSupplier<kI, vI, kO, vO> extends ProcessorSupplier<kI, vI, kO, vO> {
        @Override
        KSProcessor<kI, vI, kO, vO> get();
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
     *
     * @param topic
     * @param topicNameExtractor
     * @param streamPartitioner
     * @param <K>
     * @param <V>
     */
    public record SinkDefinition<K, V>(@Nonnull Topic<K, V> topic,
                                       @Nullable TopicNameExtractor<K, V> topicNameExtractor,
                                       @Nullable StreamPartitioner<K, V> streamPartitioner) {
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
        public static class CachedProcessorSupplier<kI, vI, kO, vO> implements KSProcessorSupplier<kI, vI, kO, vO> {
            private final AtomicReference<KSProcessor<kI, vI, kO, vO>> cachedProcessor = new AtomicReference<>();
            private final KSProcessorSupplier<kI, vI, kO, vO> processorSupplier;
            private final KSProcessor<kI, vI, kO, vO> metaProcessor;
            private Topic<kI, ?> dlq;
            private KSDLQTransformer<kI, vI, ?> dlqTransformer;

            public CachedProcessorSupplier(KSProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
                this.processorSupplier = processorSupplier;

                metaProcessor = getProcessor();
                this.cachedProcessor.set(metaProcessor);
            }

            @SuppressWarnings({"unchecked", "rawtypes"})
            private KSProcessor<kI, vI, kO, vO> getProcessor() {
                KSProcessor<kI, vI, kO, vO> ksProcessor = processorSupplier.get();
                ksProcessor.setDLQRecordGenerator((Topic) dlq, dlqTransformer);
                return ksProcessor;
            }

            @Override
            public KSProcessor<kI, vI, kO, vO> get() {
                return Optional.ofNullable(cachedProcessor.getAndSet(null))
                        .orElse(getProcessor());
            }

            String getProcessorSimpleClassName() {
                Class<?> pClass = metaProcessor.getClass();
                return pClass.getName().replace(pClass.getPackageName() + ".", "");
            }

            public <DLQm> void setDLQ(Topic<kI, DLQm> dlq, KSDLQTransformer<kI, vI, ?super DLQm> dlqTransformer) {
                this.dlq = dlq;
                this.dlqTransformer = dlqTransformer;
            }
        }

        private final KSTopology ksTopology;
        private final CachedProcessorSupplier<kI, vI, kO, vO> processorSupplier;
        private final Set<SourceDefinition<kI, ? extends vI>> sources = new HashSet<>();
        private final Set<SinkDefinition<? extends kO, ? extends vO>> sinks = new HashSet<>();
        private Set<StoreBuilder<?>> stores = new HashSet<>();

        public ProcessorDefinition(KSTopology ksTopology, KSProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
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
            this.stores = Arrays.stream(stores).collect(Collectors.toSet());
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

        /**
         * Adds DQL to processor.
         *
         * @param dlq topic
         * @param dlqTransformer transforms failed income message into a DLQ record
         * @return processor builder
         * @param <DLQm> dlq topic value type
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <DLQm> ProcessorDefinition<kI, vI, kO, vO> withDLQ(Topic<kI, DLQm> dlq, KSDLQTransformer<kI, vI, ? super DLQm> dlqTransformer) {
            this.processorSupplier.setDLQ(dlq, dlqTransformer);
            return withSink((Topic) dlq);
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
     * @see ProcessorDefinition#withDLQ(Topic, KSDLQTransformer)
     */
    public <kI, vI, kO, vO> ProcessorDefinition<kI, vI, kO, vO> addProcessor(KSProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
        return new ProcessorDefinition<>(this, processorSupplier);
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull KSProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source) {
        return addProcessor(processorSupplier, source, null, new StoreBuilder[]{});
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull KSProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source,
            @Nonnull StoreBuilder<?>... stores) {
        return addProcessor(processorSupplier, source, null, stores);
    }

    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull KSProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
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
     * @see #addProcessor(KSProcessorSupplier, Topic)
     * @see #addProcessor(KSProcessorSupplier, Topic, Topic)
     * @see #addProcessor(KSProcessorSupplier, Topic, StoreBuilder[])
     * @see #addProcessor(KSProcessorSupplier, Topic, Topic, StoreBuilder[])
     */
    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull KSProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
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

    public static class TopologyNameGenerator {
        @Nonnull
        public static String processorName(ProcessorDefinition.CachedProcessorSupplier<?, ?, ?, ?> supplier) {
            return "prc__" + supplier.getProcessorSimpleClassName();
        }

        @Nonnull
        public static String sinkName(Topic<?, ?> topic) {
            return "dst__" + topic.topic;
        }

        @Nonnull
        public static String sourceName(Topic<?, ?> topic) {
            return "src__" + topic.topic;
        }
    }
}
