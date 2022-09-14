package extb.gba.instrument.normalizer;

import extb.gba.instrument.normalizer.messages.InstrumentDefinition;
import extb.gba.instrument.normalizer.messages.SeekRequest;
import extb.gba.instrument.normalizer.messages.UsStreetExecution;
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

    public static Topic<String, UsStreetExecution> UPSTREAM;
    public static Topic<Long, UsStreetExecution> INSTRUMENT_ENRICHED;
    public static Topic<String, UsStreetExecution> INSTRUMENT_MISSED;
    public static Topic<String, SeekRequest> INSTRUMENT_SEEK_REQUEST;
    public static Topic<String, InstrumentDefinition> INSTRUMENT_UPDATED;

    static {
        JsonSerde<UsStreetExecution> executionJsonSerde = new JsonSerde<>(UsStreetExecution.class);
        Topics.UPSTREAM = new Topic<>("gba.upstream.domain.us.street.tradeExecuted", Serdes.String(), executionJsonSerde);

        Topics.INSTRUMENT_ENRICHED = new Topic<>("gba.instrument.domain.us.street.instrumentEnriched", Serdes.Long(), executionJsonSerde);

        Topics.INSTRUMENT_MISSED = new Topic<>("gba.instrument.internal.us.street.instrumentMissed", Serdes.String(), executionJsonSerde);

        JsonSerde<SeekRequest> seekRequestSerde = new JsonSerde<>(SeekRequest.class);
        Topics.INSTRUMENT_SEEK_REQUEST = new Topic<>("gba.instrument.seek.internal.lookupRequested", Serdes.String(), seekRequestSerde);

        JsonSerde<InstrumentDefinition> instrumentSerde = new JsonSerde<>(InstrumentDefinition.class);
        Topics.INSTRUMENT_UPDATED = new Topic<>("gba.instrument.domain.equity.instrumentUpdated", Serdes.String(), instrumentSerde);
    }
}
