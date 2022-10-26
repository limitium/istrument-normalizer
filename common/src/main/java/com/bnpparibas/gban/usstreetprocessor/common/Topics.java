package com.bnpparibas.gban.usstreetprocessor.common;

import com.bnpparibas.gban.usstreetprocessor.common.messages.TigerReply;
import com.bnpparibas.gban.usstreetprocessor.common.messages.UsStreetExecution;
import com.bnpparibas.gban.kscore.kstreamcore.Topic;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

public class Topics {
    public static final String EXECUTION_REPORTS_TOPIC = "gba.instrument.domain.us-street.instrumentUpdated";
    public static final String BOOK_ENRICHED_TOPIC = "gba.us-street-processor.internal.bookEnriched";
    public static final String BOOK_TIGER_TOPIC = "gba.us-street-processor.domain.bookTiger";
    public static final String TIGER_REPLY_CSV_TOPIC = "gba.us-street-processor.domain.tigerCSVReplied";
    public static final String TIGER_REPLY_TOPIC = "gba.us-street-processor.internal.tigerReplied";
    public static final String DLQ_TOPIC = "gba.dlq.domain.us.street.executionReport";
    public static Topic<Long, UsStreetExecution> EXECUTION_REPORTS;
    public static Topic<Long, UsStreetExecution> BOOK_ENRICHED;
    public static Topic<Long, String> BOOK_TIGER;
    public static Topic<Long, String> TIGER_REPLY_CSV;
    public static Topic<Long, TigerReply> TIGER_REPLY;
    public static Topic<Long, UsStreetExecution> DLQ;

    static {
        JsonSerde<UsStreetExecution> executionJsonSerde = new JsonSerde<>(UsStreetExecution.class);
        JsonSerde<TigerReply> tigerReplyJsonSerde = new JsonSerde<>(TigerReply.class);

        Topics.EXECUTION_REPORTS =
                new Topic<>(
                        EXECUTION_REPORTS_TOPIC,
                        Serdes.Long(),
                        executionJsonSerde);

        Topics.BOOK_ENRICHED =
                new Topic<>(
                        BOOK_ENRICHED_TOPIC,
                        Serdes.Long(),
                        executionJsonSerde);

        Topics.BOOK_TIGER =
                new Topic<>(
                        BOOK_TIGER_TOPIC,
                        Serdes.Long(),
                        Serdes.String());

        Topics.TIGER_REPLY_CSV =
                new Topic<>(
                        TIGER_REPLY_CSV_TOPIC,
                        Serdes.Long(),
                        Serdes.String());

        Topics.TIGER_REPLY =
                new Topic<>(
                        TIGER_REPLY_TOPIC,
                        Serdes.Long(),
                        tigerReplyJsonSerde);

        Topics.DLQ =
                new Topic<>(
                        DLQ_TOPIC,
                        Serdes.Long(),
                        executionJsonSerde);
    }
}
