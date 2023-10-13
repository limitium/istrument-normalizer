package com.limitium.gban;

import com.limitium.gban.kscore.KStreamApplication;
import com.limitium.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import com.limitium.gban.kscore.test.BaseKStreamApplicationTests;
import com.limitium.gban.kscore.test.KafkaTest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@KafkaTest(topics = {"tcp1", "tpc2"}, consumers = {"tpc2"}, configs = {
        KStreamApplication.class,
        BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
        StorageTests.TopologyConfig.class
})
class StorageTests extends BaseKStreamApplicationTests {
    public static final String SRC_TOPIC = "tpc";
    public static final String STR_KV = "kv";
    public static final String STR_IN = "in";
    public static final String PRC_UPD = "prc";
    public static JsonSerde<User> userJsonSerde;

    static {
        userJsonSerde = new JsonSerde<>(User.class);

    }

    public static class User {
        public long id;
        public String name;
        public String address;
        public String occupation;

        public User() {

        }


        public User(long id, String name, String address, String occupation) {

            this.id = id;
            this.name = name;
            this.address = address;
            this.occupation = occupation;
        }
    }

    private static class StateSaver implements Processor<Long, User, Long, User> {
        private IndexedKeyValueStore<Long, User> indexedStore;
        private ProcessorContext<Long, User> context;

        @Override
        public void init(ProcessorContext<Long, User> context) {
            Processor.super.init(context);
            this.context = context;
            IndexedMeteredKeyValueStore<Long, User> indexedStore = context.getStateStore(STR_KV);


            indexedStore.onPostInit(null);
            System.out.println("Inited");
        }

        @Override
        public void process(Record<Long, User> record) {
            indexedStore.put(record.key(), record.value());

            User user = indexedStore.getUnique("IDX_NAME", record.value().name);

            System.out.println("record");
            System.err.println(user);
        }
    }

    @Configuration
    public static class TopologyConfig {
        @Bean
        KStreamInfraCustomizer.KStreamTopologyBuilder provideTopology() {
            IndexedKeyValueStoreBuilder<Long, User> normalKV = Stores2.keyValueStoreBuilder(Stores.persistentKeyValueStore(STR_KV), Serdes.Long(), userJsonSerde).addUniqIndex("IDX_NAME", user -> user.name);


            return topology -> topology.addSource(SRC_TOPIC, Serdes.Long().deserializer(), userJsonSerde.deserializer(), "tpc1")

                    // add two processors one per source
                    .addProcessor(PRC_UPD, StateSaver::new, SRC_TOPIC)

                    // add store to both processors
                    .addStateStore(normalKV, PRC_UPD);
        }
    }

    @Test
    void happyPath() {
        User user = new User(1L, "abc123", "address1", "aaa");
        User user2 = new User(2L, "abc123", "address1", "bbb");

        send("tpc1", Serdes.Long().serializer().serialize(null, user.id), userJsonSerde.serializer().serialize(null, user));
        send("tpc1", Serdes.Long().serializer().serialize(null, user2.id), userJsonSerde.serializer().serialize(null, user2));

//        waitForRecordFrom("tpc2");

    }
}
