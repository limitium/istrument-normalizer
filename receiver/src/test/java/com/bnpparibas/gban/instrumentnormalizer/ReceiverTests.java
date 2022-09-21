package com.bnpparibas.gban.instrumentnormalizer;

import com.bnpparibas.gban.communication.messages.domain.executionreports.flatbuffers.*;
import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.bnpparibas.gban.flatbufferstooling.communication.NormalizeInstrument;
import com.bnpparibas.gban.flatbufferstooling.communication.util.PrimitiveNulls;
import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "gba.upstream.domain.usstreetcash.execution.normalizeInstrument",
                "gba.instrument.internal.instrumentLookuped",
                "happy.path.topic"
        },
        consumers = {
                "happy.path.topic"
        })
class ReceiverTests extends BaseKStreamApplicationTests {
    @MockBean
    @Autowired
    InstrumentKeeper keeper;

    @Test
    void happyPath() {
        Mockito.when(keeper.lookupInstrumentId(Mockito.anyString())).thenReturn(123L);

        FbUsStreetExecutionReport fbUsStreetExecutionReport = generateExecutionReport("IBM");
        String securityId = fbUsStreetExecutionReport.order().securityId();

        FBNormalizeInstrument normalizeInstrument = NormalizeInstrument.wrapWithNormalizeInstrument(fbUsStreetExecutionReport, fbUsStreetExecutionReport.executionReport().id(), securityId, "order", "happy.path.topic");

        send("gba.upstream.domain.usstrreetcash.execution.normalizeInstrument", securityId.getBytes(StandardCharsets.UTF_8), normalizeInstrument.getByteBuffer().array());

        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom("happy.path.topic");

        Long instrumentId = Serdes.Long().deserializer().deserialize(null, record.key());
        assertEquals(123L, instrumentId);

        FbUsStreetExecutionReport report = FbUsStreetExecutionReport.getRootAsFbUsStreetExecutionReport(ByteBuffer.wrap(record.value()));
        assertEquals(instrumentId, report.order().instrumentId());

        ensureEmptyTopic("happy.path.topic");
    }


    private static int MAX_LENGTH = 20;

    static FbUsStreetExecutionReport generateExecutionReport(String securityId) {
        final Random random = new Random();

        FlatBufferBuilder builder = new FlatBufferBuilder().forceDefaults(true);
        int convertFbOrder = FbOrder.createFbOrder(builder,
                (byte) random.nextInt(FbSide.names.length),
                (byte) random.nextInt(FbOrderType.names.length),
                (byte) random.nextInt(FbTimeInForce.names.length),
                (byte) random.nextInt(FbCapacity.names.length),
                builder.createString(securityId),
                PrimitiveNulls.NULL_LONG,
                builder.createString(randomString(random)),
                PrimitiveNulls.NULL_LONG);
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
