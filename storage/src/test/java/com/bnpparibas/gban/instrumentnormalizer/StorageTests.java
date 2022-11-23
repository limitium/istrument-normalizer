package com.bnpparibas.gban.instrumentnormalizer;

import com.bnpparibas.gban.kscore.test.BaseKStreamApplicationTests;
import com.bnpparibas.gban.kscore.test.KafkaTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

@KafkaTest(
        topics = {"tcp1", "tpc2"},
        consumers = {"tpc2"})
class StorageTests extends BaseKStreamApplicationTests {

    @Test
    void happyPath() {

        send(
                "tpc1",
                "k".getBytes(StandardCharsets.UTF_8),
                "v".getBytes(StandardCharsets.UTF_8));

        waitForRecordFrom("tpc2");

    }
}
