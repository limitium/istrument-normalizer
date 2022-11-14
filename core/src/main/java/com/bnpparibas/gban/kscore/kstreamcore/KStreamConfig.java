package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KStreamConfig {

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootStrapServers;

    /**
     * Sets kafka client configuration
     *
     * @param appName
     * @return
     */
    @Bean(name =
            KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(
            @Value("${spring.application.name}") String appName) {
        return new KafkaStreamsConfiguration(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, appName,
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers,
                        StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "-1",
                        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2",
                        CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "2000",
                        CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "6000",
                        StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, "gba." + appName + ".store"
                ));
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer kStreamsFactoryConfigurer(KafkaStreamsInfrastructureCustomizer topologyProvider) {
        return factoryBean -> factoryBean.setInfrastructureCustomizer(topologyProvider);
    }
}