package com.bnpparibas.gban.usstreetprocessor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

public class TigerReply {
    long tradeId;
    TigerAllocation.State state;

    public static class Topics {
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
        public static Topic<Long, String> TIGER_BOOKED;
        public static Topic<Long, String> TIGER_REPLY;
        public static Topic<Long, UsStreetExecution> DLQ;

        static {
            JsonSerde<UsStreetExecution> executionJsonSerde = new JsonSerde<>(UsStreetExecution.class);
            Topics.EXECUTION_REPORTS = new Topic<>("gba.instrument.domain.us-street.instrumentUpdated", Serdes.Long(), executionJsonSerde);
            Topics.BOOK_ENRICHED = new Topic<>("gba.us-street-processor.internal.bookEnriched", Serdes.Long(), executionJsonSerde);
            Topics.TIGER_BOOKED = new Topic<>("gba.us-street-processor.domain.tigerBooked", Serdes.Long(), Serdes.String());
            Topics.TIGER_REPLY = new Topic<>("gba.us-street-processor.domain.tigerReplied", Serdes.Long(), Serdes.String());

            Topics.DLQ = new Topic<>("gba.dlq.domain.us.street.executionReport", Serdes.Long(), executionJsonSerde);
        }
    }
}
