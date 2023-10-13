//package com.limitium.gban.instrumentnormalizer;
//
//
//import com.limitium.gban.instrumentnormalizer.Storage.User;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.Serdes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Properties;
//
//import static com.limitium.gban.instrumentnormalizer.Storage.userJsonSerde;
//
///**
// * The simplest implementation of a kafka message producer
// */
//public class EventGenerator {
//    private static final Logger logger = LoggerFactory.getLogger(EventGenerator.class);
//
//    public static void main(String[] args) {
//
//
//        // create instance for properties to access producer configs
//        Properties props = new Properties();
//
//        //Assign localhost id
//        props.put("bootstrap.servers", "localhost:9092");
//
//        //Set acknowledgements for producer requests.
//        props.put("acks", "all");
//
//        //If the request fails, the producer can automatically retry,
//        props.put("retries", 0);
//
//        //Specify buffer size in config
//        props.put("batch.size", 16384);
//
//        //Reduce the no of requests less than 0
//        props.put("linger.ms", 1);
//
//        //The buffer.memory controls the total amount of memory available to the producer for buffering.
//        props.put("buffer.memory", 33554432);
//
//
//        Producer<Long, User> producer = new KafkaProducer<>(props, Serdes.Long().serializer(), userJsonSerde.serializer());
//        logger.info("Sending");
//
//        generateEvent(producer, 0L, "aaa", "aaa", "aaa");
//        generateEvent(producer, 1L, "bbb", "bbb", "bbb");
//        generateEvent(producer, 2L, "ccc", "ccc", "ccc");
//
//        logger.info("Message sent successfully");
//        producer.close();
//    }
//
//    private static void generateEvent(Producer<Long, User> producer, long i, String name, String address, String occupation) {
//        User user = new User(i, name, address, occupation);
//        ProducerRecord<Long, User> record = new ProducerRecord<>("tpc1", i, user);
//        logger.info(user.toString());
//        producer.send(record);
//    }
//}