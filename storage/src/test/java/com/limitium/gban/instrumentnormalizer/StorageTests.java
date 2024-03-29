//package com.limitium.gban.instrumentnormalizer;
//
//import com.limitium.gban.kscore.test.BaseKStreamApplicationTests;
//import com.limitium.gban.kscore.test.KafkaTest;
//import org.apache.kafka.common.serialization.Serdes;
//import org.junit.jupiter.api.Test;
//
//
//@KafkaTest(
//        topics = {"tcp1", "tpc2"},
//        consumers = {"tpc2"})
//class StorageTests extends BaseKStreamApplicationTests {
//
//    @Test
//    void happyPath() {
//        com.limitium.gban.StorageTests.User user = new User(1L, "abc123", "address1", "aaa");
//        User user2 = new User(2L, "abc123", "address1", "bbb");
//
//        send(
//                "tpc1",
//                Serdes.Long().serializer().serialize(null, user.id),
//                userJsonSerde.serializer().serialize(null, user));
//        send(
//                "tpc1",
//                Serdes.Long().serializer().serialize(null, user2.id),
//                userJsonSerde.serializer().serialize(null, user2));
//
//        waitForRecordFrom("tpc2");
//
//    }
//}
