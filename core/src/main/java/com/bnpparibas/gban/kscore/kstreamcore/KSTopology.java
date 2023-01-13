package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
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

    record SourceDefinition<K, V>(@Nonnull Topic<K, V> topic, boolean isPattern) {
    }

    record SinkDefinition<K, V>(@Nonnull Topic<K, V> topic,
                                @Nullable TopicNameExtractor<K, V> topicNameExtractor,
                                @Nullable StreamPartitioner<K, V> streamPartitioner) {
    }

    public static class ProcessorDefinition<kI, vI, kO, vO> {

        private final KSTopology ksTopology;
        private final KSProcessorSupplier<kI, vI, kO, vO> processorSupplier;
        private Set<SourceDefinition<kI, vI>> sources = new HashSet<>();
        private Set<SinkDefinition<kO, vO>> sinks = new HashSet<>();
        private Set<StoreBuilder<?>> stores;

        public ProcessorDefinition(KSTopology ksTopology, KSProcessorSupplier<kI, vI, kO, vO> processorSupplier) {
            this.ksTopology = ksTopology;
            this.processorSupplier = processorSupplier;
        }

        /**
         * Connects processor with the source topic
         *
         * @param topic
         * @return
         */
        public ProcessorDefinition<kI, vI, kO, vO> withSource(Topic<kI, vI> topic) {
            return withSource(new SourceDefinition<>(topic, false));
        }

        public ProcessorDefinition<kI, vI, kO, vO> withSource(SourceDefinition<kI, vI> source) {
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
         * Connects processor with the sink topic
         *
         * @param topic
         * @return
         */
        public ProcessorDefinition<kI, vI, kO, vO> withSink(Topic<kO, vO> topic) {
            return withSink(new SinkDefinition<>(topic, null, null));
        }

        public ProcessorDefinition<kI, vI, kO, vO> withSink(SinkDefinition<kO, vO> sink) {
            sinks.add(sink);
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
     * Limitation - no processor chaining
     *
     * @param processorSupplier
     * @param <kI>
     * @param <vI>
     * @param <kO>
     * @param <vO>
     * @return
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
     * Creates processor with a single source, single sink and stores
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
     */
    public <kIl, vIl, kOl, vOl> KSTopology addProcessor(
            @Nonnull KSProcessorSupplier<kIl, vIl, kOl, vOl> processorSupplier,
            @Nonnull Topic<kIl, vIl> source,
            @Nullable Topic<kOl, vOl> sink,
            @Nullable StoreBuilder<?>... stores) {

        ProcessorDefinition<kIl, vIl, kOl, vOl> processorDefinition = new ProcessorDefinition<>(this, processorSupplier)
                .withSource(source);

        if (sink != null) {
            processorDefinition.withSink(sink);
        }
        if (stores != null && stores.length > 0) {
            processorDefinition.withStores(stores);
        }
        return processorDefinition.done();
    }


    @SuppressWarnings("unchecked")
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
        public static String processorName(KSTopology.KSProcessorSupplier<?, ?, ?, ?> processor) {
            return "prc__" + processor.getClass().getSimpleName();
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
