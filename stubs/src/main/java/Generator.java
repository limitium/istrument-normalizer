//import util.properties packages

import extb.gba.instrument.normalizer.Topics;
import extb.gba.instrument.normalizer.external.InstrumentKeeper;
import extb.gba.instrument.normalizer.messages.UsStreetExecution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//Create java class named “SimpleProducer”
public class Generator {
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);

    public static void main(String[] args) {


        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        Producer<String, UsStreetExecution> producer = new KafkaProducer<>(props, Topics.UPSTREAM.keySerde.serializer(), Topics.UPSTREAM.valueSerde.serializer());
        logger.info("Sending");
        List<String> upstreamSecurities = new ArrayList<>(InstrumentKeeper.SECURITIES);
        upstreamSecurities.add("TSLA");
        upstreamSecurities.add("NTFX");

        for (int i = 0; i < 1_000; i++) {
            UsStreetExecution execution = new UsStreetExecution();

            execution.securityId = upstreamSecurities.get(i % upstreamSecurities.size());
            execution.executionId = System.nanoTime();
            execution.qty = Math.random();
            execution.price = Math.random();

            ProducerRecord<String, UsStreetExecution> record = new ProducerRecord<>(Topics.UPSTREAM.topic, execution.securityId, execution);
            logger.info(record.toString());
            producer.send(record);
        }
        logger.info("Message sent successfully");
        producer.close();
    }
}