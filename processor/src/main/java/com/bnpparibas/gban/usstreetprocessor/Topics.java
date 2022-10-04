package com.bnpparibas.gban.usstreetprocessor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

public class Topics {
    public static class Topic<K, V> {
        public String topic;
        public Serde<K> keySerde;
        public Serde<V> valueSerde;

        public Topic(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
            this.topic = topic;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

    }

    public static Topic<Long, UsStreetExecution> EXECUTION_REPORTS;
    public static Topic<Long, UsStreetExecution> BOOK_ENRICHED;
    public static Topic<Long, String> BOOK_TIGER;
    public static Topic<String, String> TIGER_REPLY_CSV;
    public static Topic<Long, TigerReply> TIGER_REPLY;
    public static Topic<Long, UsStreetExecution> DLQ;

    static {
        JsonSerde<UsStreetExecution> executionJsonSerde = new JsonSerde<>(UsStreetExecution.class);
        JsonSerde<TigerReply> tigerReplyJsonSerde = new JsonSerde<>(TigerReply.class);

        Topics.EXECUTION_REPORTS = new Topic<>("gba.instrument.domain.us-street.instrumentUpdated", Serdes.Long(), executionJsonSerde);
        Topics.BOOK_ENRICHED = new Topic<>("gba.us-street-processor.internal.bookEnriched", Serdes.Long(), executionJsonSerde);
        Topics.BOOK_TIGER = new Topic<>("gba.us-street-processor.domain.bookTiger", Serdes.Long(), Serdes.String());

        Topics.TIGER_REPLY_CSV = new Topic<>("gba.us-street-processor.domain.tigerCSVReplied", Serdes.String(), Serdes.String());
        Topics.TIGER_REPLY = new Topic<>("gba.us-street-processor.internal.tigerReplied", Serdes.Long(), tigerReplyJsonSerde);

        Topics.DLQ = new Topic<>("gba.dlq.domain.us.street.executionReport", Serdes.Long(), executionJsonSerde);
    }
}
