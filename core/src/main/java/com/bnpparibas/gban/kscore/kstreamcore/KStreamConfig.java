package com.bnpparibas.gban.kscore.kstreamcore;

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
     * @param kafkaStorePrefix used in {@link ProcessorStateManager#changelogFor(String)}
     * @return
     */
    @Bean(name =
            KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(
            @Value("${spring.application.name}") String appName,
            @Value("${kafka.store.prefix:#{null}}") String kafkaStorePrefix
    ) {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, appName,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers,
                StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, kafkaStorePrefix != null ? kafkaStorePrefix + "." + appName.toLowerCase() + ".store" : appName
        ));
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer kStreamsFactoryConfigurer(KafkaStreamsInfrastructureCustomizer topologyProvider) {
        return factoryBean -> factoryBean.setInfrastructureCustomizer(topologyProvider);
    }
}