package com.bnpparibas.gban.instrumentnormalizer;


import com.bnpparibas.gban.kscore.kstreamcore.KStreamInfraCustomizer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


@Component
public class Storage implements KStreamInfraCustomizer.KStreamTopologyBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Storage.class);

    public static final String SRC_TOPIC = "tpc";
    public static final String STR_KV = "kv";
    public static final String STR_IN = "in";
    public static final String PRC_UPD = "prc";

    @Override
    public void configureTopology(Topology topology) {
        StoreBuilder<KeyValueStore<String, String>> normalKV =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STR_KV),
                        Serdes.String(),
                        Serdes.String()
                );

        topology
                .addSource(SRC_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer(), "tpc1")

                // add two processors one per source
                .addProcessor(PRC_UPD, StateSaver::new, SRC_TOPIC)

                // add store to both processors
                .addStateStore(normalKV, PRC_UPD);
    }


    private static class StateSaver implements Processor<String, String, Object, Object> {
        private KeyValueStore<String, String> normalKV;
        private ProcessorContext<Object, Object> context;

        @Override
        public void init(ProcessorContext<Object, Object> context) {
            Processor.super.init(context);
            this.context = context;
            normalKV = context.getStateStore(STR_KV);
            System.out.println(1);
        }

        @Override
        public void process(Record<String, String> record) {
            normalKV.put(record.key(), record.value());
        }

    }
}
