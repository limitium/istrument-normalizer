package com.bnpparibas.gban.kscore;

import com.bnpparibas.gban.kscore.kstreamcore.KSProcessor;
import com.bnpparibas.gban.kscore.kstreamcore.KSTopology;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KSTopologyDescriptionTest {


    static class A {
    }

    static class B extends A {
    }

    static class C extends B {
    }

    static class Z {
    }

    static class Y1 extends Z {
    }

    static class Y2 extends Z {
    }

    static Topic<Integer, A> topicIntA = new Topic<>("topicIntA", Serdes.Integer(), new Serde<A>() {
        @Override
        public Serializer<A> serializer() {
            return null;
        }

        @Override
        public Deserializer<A> deserializer() {
            return null;
        }
    });
    static Topic<Integer, B> topicIntBPattern = new Topic<>("topicIntBPattern*", Serdes.Integer(), new Serde<B>() {
        @Override
        public Serializer<B> serializer() {
            return null;
        }

        @Override
        public Deserializer<B> deserializer() {
            return null;
        }
    });

    static Topic<Integer, C> topicIntC = new Topic<>("topicIntC", Serdes.Integer(), new Serde<>() {
        @Override
        public Serializer<C> serializer() {
            return null;
        }

        @Override
        public Deserializer<C> deserializer() {
            return null;
        }
    });


    static Topic<Long, Z> topicLongZ = new Topic<>("topicLongZ", Serdes.Long(), new Serde<Z>() {
        @Override
        public Serializer<Z> serializer() {
            return null;
        }

        @Override
        public Deserializer<Z> deserializer() {
            return null;
        }
    });
    static Topic<Long, Y1> topicLongY1 = new Topic<>("topicLongY1", Serdes.Long(), new Serde<Y1>() {
        @Override
        public Serializer<Y1> serializer() {
            return null;
        }

        @Override
        public Deserializer<Y1> deserializer() {
            return null;
        }
    });
    static Topic<Long, Y2> topicLongY2 = new Topic<>("topicLongY2", Serdes.Long(), new Serde<Y2>() {
        @Override
        public Serializer<Y2> serializer() {
            return null;
        }

        @Override
        public Deserializer<Y2> deserializer() {
            return null;
        }
    });

    static Topic<Integer, B> dlq = new Topic<>("dlq", Serdes.Integer(), new Serde<B>() {
        @Override
        public Serializer<B> serializer() {
            return null;
        }

        @Override
        public Deserializer<B> deserializer() {
            return null;
        }
    });

    static class TestProcessor extends KSProcessor<Integer, A, Long, Z> {
        @Override
        public void process(Record<Integer, A> record) {
            send(topicLongY1, record.withValue(new Y1()).withKey(1L));
            send(topicLongZ, record.withValue(new Z()).withKey(1L));
        }
    }

    @Test
    void build() {
        Topology topology = new Topology();
        KSTopology ksTopology = new KSTopology(topology);


        ksTopology.addProcessor(TestProcessor::new)
                .withSource(topicIntC)
                .withSink(topicLongZ)
                .withSink(topicLongY1)
                .withDLQ(dlq, (failed, fromTopic, partition, offset, errorMessage, exception) -> null)
                .done()
                .addProcessor(() -> new KSProcessor<Integer, A, Long, Y2>() {
                    @Override
                    public void process(Record<Integer, A> record) {

                    }
                })
                .withSource(new KSTopology.SourceDefinition<>(topicIntBPattern, true))
                .withSink(new KSTopology.SinkDefinition<>(topicLongY2, null, (topic, key, value, numPartitions) -> null))
                .done();

        ksTopology.buildTopology();
        assertEquals(2, topology.describe().subtopologies().size());
    }
}
