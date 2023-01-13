package com.bnpparibas.gban.kscore;

import com.bnpparibas.gban.kscore.kstreamcore.KSProcessor;
import com.bnpparibas.gban.kscore.kstreamcore.KSTopology;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KSTopologyDescriptionTest {
    @Test
    void build() {
        Topology topology = new Topology();
        KSTopology ksTopology = new KSTopology(topology);

        Topic<Integer, Long> topicIntLong = new Topic<>("topicIntLong", Serdes.Integer(), Serdes.Long());
        Topic<Integer, Long> topicIntLongPattern = new Topic<>("topicPattern*", Serdes.Integer(), Serdes.Long());
        Topic<String, Object> topicStringObject = new Topic<>("topicStringObject", Serdes.String(), new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return null;
            }

            @Override
            public Deserializer<Object> deserializer() {
                return null;
            }
        });
        Topic<String, Object> topicStringObjectExtract = new Topic<>("topicStringObjectExtract", Serdes.String(), new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return null;
            }

            @Override
            public Deserializer<Object> deserializer() {
                return null;
            }
        });

        ksTopology.addProcessor(() -> new KSProcessor<Number, Number, Object, Object>() {
            @Override
            public void process(Record<Number, Number> record) {

            }
        })
                .withSource(topicIntLong)
                .withSink(topicStringObject)
                .withSink(topicIntLong)
                .done()
                .addProcessor(() -> new KSProcessor<Number, Number, Object, Object>() {
                    @Override
                    public void process(Record<Number, Number> record) {

                    }
                })
                .withSource(new KSTopology.SourceDefinition<>(topicIntLongPattern, true))
                .withSink(new KSTopology.SinkDefinition<>(topicStringObjectExtract, null, (topic, key, value, numPartitions) -> null))
                .done();

        ksTopology.buildTopology();
        assertEquals(2, topology.describe().subtopologies().size());
    }
}
