package com.limitium.gban.kscore.kstreamcore;

import com.limitium.gban.kscore.kstreamcore.KSTopology.SinkDefinition;
import com.limitium.gban.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;


public class Broadcast<V> extends SinkDefinition<Integer, V> {
    /**
     * Define broadcast. Message will be sent to all {@code topic} partitions via {@link ExtendedProcessorContext#broadcast(Broadcast, Object)}
     *
     * @param topic              definition to broadcast
     * @param topicNameExtractor
     */
    public Broadcast(Topic<Integer, V> topic, TopicNameExtractor<Integer, V> topicNameExtractor) {
        super(topic, topicNameExtractor, (topicName, key, value, numPartitions) -> key);
    }

    /**
     * @param topic
     * @see #Broadcast(Topic, TopicNameExtractor)
     */
    public Broadcast(Topic<Integer, V> topic) {
        this(topic, null);
    }
}
