package com.limitium.gban.instrumentnormalizer;

import static com.limitium.gban.instrumentnormalizer.ReceiverTests.HAPPY_PATH_TOPIC;
import static com.limitium.gban.instrumentnormalizer.ReceiverTests.UPSTREAM_DOMAIN_USSTREETCASH_EXECUTION_NORMALIZE_INSTRUMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.limitium.gban.communication.messages.domain.executionreports.flatbuffers.*;
import com.limitium.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.limitium.gban.flatbufferstooling.communication.NormalizeInstrument;
import com.limitium.gban.flatbufferstooling.communication.util.PrimitiveNulls;
import com.limitium.gban.instrumentkeeper.client.InstrumentKeeperClient;
import com.limitium.gban.kscore.test.BaseKStreamApplicationTests;
import com.limitium.gban.kscore.test.KafkaTest;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

@KafkaTest(
        topics = {
                UPSTREAM_DOMAIN_USSTREETCASH_EXECUTION_NORMALIZE_INSTRUMENT,
                Topics.INSTRUMENT_LOOKUPED_TOPIC,
                HAPPY_PATH_TOPIC
        },
        consumers = {HAPPY_PATH_TOPIC})
class ReceiverTests extends BaseKStreamApplicationTests {

    public static final String UPSTREAM_DOMAIN_USSTREETCASH_EXECUTION_NORMALIZE_INSTRUMENT =
            "gba.upstream.domain.usstreetcash.execution.normalizeInstrument";
    public static final String HAPPY_PATH_TOPIC = "happy.path.topic";

    @MockBean @Autowired InstrumentKeeperClient keeper;
    @Test
    void happyPath() {
        Mockito.when(
                        keeper.lookupIdBy(
                                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(123L);

        FbUsStreetExecutionReport fbUsStreetExecutionReport = generateExecutionReport("IBM");
        String securityId = fbUsStreetExecutionReport.order().securityId();

        FBNormalizeInstrument normalizeInstrument =
                NormalizeInstrument.wrapWithNormalizeInstrument(
                        fbUsStreetExecutionReport,
                        fbUsStreetExecutionReport.executionReport().id(),
                        securityId,
                        ".order",
                        "happy.path.topic");

        send(
                Topics.UPSTREAM,
                UPSTREAM_DOMAIN_USSTREETCASH_EXECUTION_NORMALIZE_INSTRUMENT,
                securityId,
                normalizeInstrument);

        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom(HAPPY_PATH_TOPIC);

        Long instrumentId = Serdes.Long().deserializer().deserialize(null, record.key());
        assertEquals(123L, instrumentId);

        FbUsStreetExecutionReport report =
                FbUsStreetExecutionReport.getRootAsFbUsStreetExecutionReport(
                        ByteBuffer.wrap(record.value()));
        assertEquals(instrumentId, report.order().instrumentId());

        ensureEmptyTopic(HAPPY_PATH_TOPIC);
    }


    private static int MAX_LENGTH = 20;

    static FbUsStreetExecutionReport generateExecutionReport(String securityId) {
        final Random random = new Random();

        FlatBufferBuilder builder = new FlatBufferBuilder().forceDefaults(true);
//        int convertFbOrder = FbOrder.createFbOrder(builder,
//                (byte) random.nextInt(FbSide.names.length),
//                (byte) random.nextInt(FbOrderType.names.length),
//                (byte) random.nextInt(FbTimeInForce.names.length),
//
//                builder.createString(securityId),
//                PrimitiveNulls.NULL_LONG,
//
//                builder.createString(randomString(random)),
//                PrimitiveNulls.NULL_LONG);
//
//        int convertFbFrontOffice = FbFrontOffice.createFbFrontOffice(builder,
//                builder.createString(randomString(random)),
//                builder.createString(randomString(random)),
//
//                builder.createString(randomString(random)),
//                builder.createString(randomString(random)),
//                builder.createString(randomString(random)));
//        int convertFbCounterParty = FbCounterParty.createFbCounterParty(builder,
//                (byte) random.nextInt(FbCounterpartyType.names.length),
//                builder.createString(randomString(random)),
//                (byte) random.nextInt(FbCounterpartyCodeType.names.length),
//                builder.createString(randomString(random)),
//                builder.createString(randomString(random)));
//        int convertFbExecutionReport = FbExecutionReport.createFbExecutionReport(builder,
//                System.nanoTime(),
//                (byte) random.nextInt(FbType.names.length),
//                (byte) random.nextInt(FbCapacity.names.length),
//
//                random.nextDouble(),
//                random.nextDouble(),
//
//                random.nextLong());
//
//        int reportOffset = FbUsStreetExecutionReport.createFbUsStreetExecutionReport(builder,
//                convertFbFrontOffice, convertFbOrder, convertFbCounterParty, convertFbExecutionReport);
//        builder.finish(reportOffset);

        //Truncate ByteBuffer
        return FbUsStreetExecutionReport.getRootAsFbUsStreetExecutionReport(
                ByteBuffer.wrap(builder.sizedByteArray())
        );
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
