package com.bnpparibas.gban.kscore.kstreamcore;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

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
        streamsProperties.putAll(kafkaProperties.buildConsumerProperties());
        streamsProperties.putAll(kafkaProperties.buildProducerProperties());
        streamsProperties.putAll(kafkaProperties.buildAdminProperties());

        streamsProperties.remove(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        streamsProperties.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        streamsProperties.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        streamsProperties.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        streamsProperties.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        String appName = env.getRequiredProperty("spring.application.name");
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);

        List.of(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG,
                CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG,
                CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG,
                StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                StreamsConfig.STATE_DIR_CONFIG
        ).forEach(propName -> {
            String propValue = getKafkaStreamProperty(propName, env);
            if (propValue != null){
                streamsProperties.put(propName, propValue);
            }
        });

        streamsProperties.put(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, appName + ".store");

        return new KafkaStreamsConfiguration(streamsProperties);
    }

    private String getKafkaStreamProperty(String propName, Environment env) {
        return env.getProperty("kafka.streams." + propName);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer kStreamsFactoryConfigurer(KafkaStreamsInfrastructureCustomizer topologyProvider, ApplicationContext applicationContext) {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(exception -> {
                SpringApplication.exit(applicationContext, () -> -1);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            factoryBean.setInfrastructureCustomizer(topologyProvider);
        };
    }
}