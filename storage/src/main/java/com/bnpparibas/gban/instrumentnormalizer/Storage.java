package com.bnpparibas.gban.instrumentnormalizer;


import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;


@Component
public class Storage implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Storage.class);

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

    @Override
    public void configureTopology(Topology topology) {


        IndexedKeyValueStoreBuilder<Long, User> normalKV =
                Stores2.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(STR_KV),
                                Serdes.Long(),
                                userJsonSerde
                        )
                        .addUniqIndex("IDX_NAME", user -> user.name);


        topology
                .addSource(SRC_TOPIC, Serdes.Long().deserializer(), userJsonSerde.deserializer(), "tpc1")

                // add two processors one per source
                .addProcessor(PRC_UPD, StateSaver::new, SRC_TOPIC)

                // add store to both processors
                .addStateStore(normalKV, PRC_UPD);
    }


    private static class StateSaver implements Processor<Long, User, Long, User> {
        private IndexedKeyValueStore<Long, User> indexedStore;
        private ProcessorContext<Long, User> context;

        @Override
        public void init(ProcessorContext<Long, User> context) {
            Processor.super.init(context);
            this.context = context;
            IndexedKeyValueStore<Long, User> indexedStore = ((WrappedStateStore<IndexedKeyValueStore<Long, User>, Long, User>)context.getStateStore(STR_KV)).wrapped();

            indexedStore.rebuildIndexes();
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
}
