package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.KStreamConfig.BoundedMemoryRocksDBConfig.BoundedMemoryRocksDBConfigSetter;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;
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
import java.util.function.Consumer;

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
                StreamsConfig.STATE_DIR_CONFIG,
                StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
                StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                ExtendedProcessorContext.SEQUENCER_NAMESPACE
        ).forEach(propName -> {
            String propValue = getKafkaStreamProperty(propName, env);
            if (propValue != null) {
                streamsProperties.put(propName, propValue);
            }
        });


        List.of(
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_MEMORY_CONFIG, s -> BoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_MEMORY = Long.parseLong(s)),
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.TOTAL_MEMTABLE_MEMORY_CONFIG, s -> BoundedMemoryRocksDBConfig.TOTAL_MEMTABLE_MEMORY = Long.parseLong(s)),
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.INDEX_FILTER_BLOCK_RATIO_CONFIG, s -> BoundedMemoryRocksDBConfig.INDEX_FILTER_BLOCK_RATIO = Double.parseDouble(s)),
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.BLOCK_SIZE_CONFIG, s -> BoundedMemoryRocksDBConfig.BLOCK_SIZE = Long.parseLong(s)),
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.N_MEMTABLES_CONFIG, s -> BoundedMemoryRocksDBConfig.N_MEMTABLES = Integer.parseInt(s)),
                new BoundedMemoryRocksDBConfigSetter(BoundedMemoryRocksDBConfig.MEMTABLE_SIZE_CONFIG, s -> BoundedMemoryRocksDBConfig.MEMTABLE_SIZE = Long.parseLong(s))
        ).forEach(setter -> {
            String propValue = getKafkaStreamRocksDBProperty(setter.key, env);
            if (propValue != null) {
                setter.setter.accept(propValue);
            }
        });

        streamsProperties.put(StreamsConfig.InternalConfig.TOPIC_PREFIX_ALTERNATIVE, appName + ".store");

        return new KafkaStreamsConfiguration(streamsProperties);
    }

    private String getKafkaStreamProperty(String propName, Environment env) {
        return env.getProperty("kafka.streams." + propName);
    }

    private String getKafkaStreamRocksDBProperty(String propName, Environment env) {
        return env.getProperty("kafka.streams.rocksdb." + propName);
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

    public static class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {

        private static long TOTAL_OFF_HEAP_MEMORY = 300;
        private static final String TOTAL_OFF_HEAP_MEMORY_CONFIG = "total.off.heap.memory.mb";
        private static double INDEX_FILTER_BLOCK_RATIO = 0;
        private static final String INDEX_FILTER_BLOCK_RATIO_CONFIG = "index.filter.block.ratio";
        private static long TOTAL_MEMTABLE_MEMORY = 10;
        private static final String TOTAL_MEMTABLE_MEMORY_CONFIG = "memtable.total.memory.mb";
        private static long BLOCK_SIZE = 4096;
        private static final String BLOCK_SIZE_CONFIG = "block.size";
        private static int N_MEMTABLES = 3;
        private static final String N_MEMTABLES_CONFIG = "memtable.number";
        private static long MEMTABLE_SIZE = 16;
        private static final String MEMTABLE_SIZE_CONFIG = "memtable.size.mb";

        Cache cache;
        WriteBufferManager writeBufferManager;

        boolean inited = false;

        synchronized void initOnce() {
            if (inited) {
                return;
            }
            inited = true;

            cache = new LRUCache(TOTAL_OFF_HEAP_MEMORY * 1024 * 1024, -1, false, INDEX_FILTER_BLOCK_RATIO);
            writeBufferManager = new WriteBufferManager(TOTAL_MEMTABLE_MEMORY * 1024 * 1024, cache);
        }

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            initOnce();

            BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();

            // These three options in combination will limit the memory used by RocksDB to the size passed to the block cache (TOTAL_OFF_HEAP_MEMORY)
            tableConfig.setBlockCache(cache);
            tableConfig.setCacheIndexAndFilterBlocks(true);
            options.setWriteBufferManager(writeBufferManager);

            // These options are recommended to be set when bounding the total memory
            tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
            tableConfig.setPinTopLevelIndexAndFilter(true);
            tableConfig.setBlockSize(BLOCK_SIZE);
            options.setMaxWriteBufferNumber(N_MEMTABLES);
            options.setWriteBufferSize(MEMTABLE_SIZE * 1024 * 1024);

            options.setTableFormatConfig(tableConfig);
        }

        @Override
        public void close(final String storeName, final Options options) {
            // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
        }

        public record BoundedMemoryRocksDBConfigSetter(String key, Consumer<String> setter) {
        }
    }
}