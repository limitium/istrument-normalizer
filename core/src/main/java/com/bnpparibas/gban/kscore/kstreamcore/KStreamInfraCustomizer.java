package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.*;

@Component
public class KStreamInfraCustomizer implements KafkaStreamsInfrastructureCustomizer {

    public interface KStreamDSLBuilder {
        void configureBuilder(StreamsBuilder builder);
    }

    public interface KStreamTopologyBuilder {
        void configureTopology(Topology topology);
    }

    /**
     * Entry point to describe kafka streams application topology
     */
    public interface KStreamKSTopologyBuilder {
        void configureTopology(KSTopology topology);
    }

    @Autowired(required = false)
    Set<KStreamTopologyBuilder> topologyBuilders;
    @Autowired(required = false)
    Set<KStreamDSLBuilder> dslBuilders;
    @Autowired(required = false)
    Set<KStreamKSTopologyBuilder> kSTopologyBuilders;

    @Autowired
    @Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration streamsConfig;

    @Override
    public void configureBuilder(@Nonnull StreamsBuilder builder) {
        Optional.ofNullable(dslBuilders).ifPresent(dslBuilders -> dslBuilders.forEach(dslBuilder -> dslBuilder.configureBuilder(builder)));
    }

    @Override
    public void configureTopology(@Nonnull Topology topology) {
        Optional.ofNullable(topologyBuilders).ifPresent(topologyBuilders -> topologyBuilders.forEach(topologyBuilder -> topologyBuilder.configureTopology(topology)));

        Optional.ofNullable(kSTopologyBuilders).ifPresent(kStreamKSTopologyBuilders -> kStreamKSTopologyBuilders.forEach(kStreamKSTopologyBuilder -> {
            KSTopology ksTopology = new KSTopology(topology, streamsConfig);
            kStreamKSTopologyBuilder.configureTopology(ksTopology);
            ksTopology.buildTopology();
        }));
    }
}