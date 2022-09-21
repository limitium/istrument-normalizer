package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;

@Component
public class KStreamInfraCustomizer implements KafkaStreamsInfrastructureCustomizer {

    public interface KStreamDSLBuilder {
        void configureBuilder(StreamsBuilder builder);
    }

    public interface KStreamTopologyBuilder {
        void configureTopology(Topology topology);
    }

    @Autowired(required = false)
    Set<KStreamTopologyBuilder> topologyBuilders;
    @Autowired(required = false)
    Set<KStreamDSLBuilder> dslBuilders;

    @Override
    public void configureBuilder(StreamsBuilder builder) {
        Optional.ofNullable(dslBuilders)
                .ifPresent(dslBuilders -> dslBuilders.forEach(dslBuilder -> dslBuilder.configureBuilder(builder)));
    }

    @Override
    public void configureTopology(Topology topology) {
        Optional.ofNullable(topologyBuilders)
                .ifPresent(topologyBuilders -> topologyBuilders.forEach(topologyBuilder -> topologyBuilder.configureTopology(topology)));
    }
}