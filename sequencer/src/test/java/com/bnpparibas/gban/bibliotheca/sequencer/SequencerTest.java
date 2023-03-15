package com.bnpparibas.gban.bibliotheca.sequencer;

import org.junit.jupiter.api.Test;

import static com.bnpparibas.gban.bibliotheca.sequencer.Sequencer.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

class SequencerTest {

    @Test
    void basicTest() {
        Sequencer sequencer = new Sequencer(() -> EPOCH_RESET + 1, 1, 1);

        String bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000100000000000", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000100000000001", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000100000000010", bits);
    }

    //todo: test overflow
    @Test
    void parseTest() {
        long currentTimeMillis = System.currentTimeMillis();
        String formattedTime = DateTimeFormatter.ofPattern(TIME_PATTERN).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMillis), ZONE_OFFSET));

        Sequencer sequencer = new Sequencer(() -> currentTimeMillis, 2, 3);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"2\",partition:\"3\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));

        sequencer = new Sequencer(() -> currentTimeMillis, 1, 2);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"1\",partition:\"2\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"1\",partition:\"2\", sequence:\"1\"}", Sequencer.parse(sequencer.getNext()));
    }
}
