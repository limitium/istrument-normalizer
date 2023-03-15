package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
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

    /**
     * Sets kafka client configuration
     *
     * @param kafkaProperties
     * @param env
     * @return
     */
    @Bean(name =
            KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(KafkaProperties kafkaProperties, Environment env) {
        Map<String, Object> streamsProperties = kafkaProperties.buildStreamsProperties();

        String appName = env.getRequiredProperty("spring.application.name");
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);

        streamsProperties.put(StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "-1");
        streamsProperties.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        streamsProperties.put(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG, "30000");
        streamsProperties.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "6000");

        streamsProperties.put(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, "gba." + appName + ".store");

        return new KafkaStreamsConfiguration(streamsProperties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer kStreamsFactoryConfigurer(KafkaStreamsInfrastructureCustomizer topologyProvider) {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(exception -> {
                factoryBean.stop();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            factoryBean.setInfrastructureCustomizer(topologyProvider);
        };
    }
}