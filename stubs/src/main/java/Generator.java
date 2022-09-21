//import util.properties packages

import com.bnpparibas.gban.communication.messages.domain.executionreports.flatbuffers.*;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.bnpparibas.gban.flatbufferstooling.communication.NormalizeInstrument;
import com.bnpparibas.gban.instrumentnormalizer.InstrumentKeeper;
import com.bnpparibas.gban.instrumentnormalizer.Topics;
import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

//Create java class named “SimpleProducer”
public class Generator {
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);
    private static int MAX_LENGTH = 20;

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

        Producer<String, FBNormalizeInstrument> producer = new KafkaProducer<>(props, Topics.UPSTREAM.keySerde.serializer(), Topics.UPSTREAM.valueSerde.serializer());
        logger.info("Sending");
        List<String> upstreamSecurities = new ArrayList<>(InstrumentKeeper.SECURITIES);
        upstreamSecurities.add("TSLA");
        upstreamSecurities.add("NTFX");
        final Random random = new Random();

        for (int i = 0; i < 1_000; i++) {
            FlatBufferBuilder builder = new FlatBufferBuilder();
            int convertFbOrder = FbOrder.createFbOrder(builder,
                    (byte) random.nextInt(FbSide.names.length),
                    (byte) random.nextInt(FbOrderType.names.length),
                    (byte) random.nextInt(FbTimeInForce.names.length),
                    (byte) random.nextInt(FbCapacity.names.length),
                    builder.createString(upstreamSecurities.get(i % upstreamSecurities.size())),
                    0L,
                    builder.createString(randomString(random)),
                    0L);
            int convertFbExecutionSystem = FbExecutionSystem.createFbExecutionSystem(builder,
                    builder.createString(randomString(random)),
                    builder.createString(randomString(random)),
                    builder.createString(randomString(random)),
                    builder.createString(randomString(random)));
            int convertFbFrontOffice = FbFrontOffice.createFbFrontOffice(builder,
                    convertFbExecutionSystem,
                    builder.createString(randomString(random)),
                    builder.createString(randomString(random)));
            int convertFbCounterParty = FbCounterParty.createFbCounterParty(builder,
                    (byte) random.nextInt(FbCounterpartyType.names.length),
                    builder.createString(randomString(random)),
                    (byte) random.nextInt(FbCounterpartyCodeType.names.length),
                    builder.createString(randomString(random)),
                    builder.createString(randomString(random)));
            int convertFbExecutionReport = FbExecutionReport.createFbExecutionReport(builder,
                    System.nanoTime(),
                    (byte) random.nextInt(FbType.names.length),
                    random.nextDouble(),
                    random.nextDouble(),
                    random.nextLong(),
                    builder.createString(randomString(random)));

            int reportOffset = FbUsStreetExecutionReport.createFbUsStreetExecutionReport(builder,
                    convertFbFrontOffice, convertFbOrder, convertFbCounterParty, convertFbExecutionReport);
            builder.finish(reportOffset);

            //Truncate ByteBuffer
            FbUsStreetExecutionReport fbUsStreetExecutionReport = FbUsStreetExecutionReport.getRootAsFbUsStreetExecutionReport(
                    ByteBuffer.wrap(builder.sizedByteArray())
            );

            String securityId = fbUsStreetExecutionReport.order().securityId();

            FBNormalizeInstrument normalizeInstrument = NormalizeInstrument.wrapWithNormalizeInstrument(fbUsStreetExecutionReport, fbUsStreetExecutionReport.executionReport().id(), securityId, "order", "happy.path.topic");

            ProducerRecord<String, FBNormalizeInstrument> record = new ProducerRecord<>("gba.upstream.domain.usstreet.execreport.normalizeInstrument", securityId, normalizeInstrument);

            logger.info(record.toString());
            producer.send(record);
        }
        logger.info("Message sent successfully");
        producer.close();
    }

    private static String randomString(Random random) {
        int leftLimit = 97;
        int rightLimit = 122;
        int length = random.nextInt(MAX_LENGTH + 1);

        return random.ints(leftLimit, rightLimit + 1)
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}