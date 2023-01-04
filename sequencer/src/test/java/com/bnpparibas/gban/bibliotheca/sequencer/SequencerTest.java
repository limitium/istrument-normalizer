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
        Sequencer sequencer = new Sequencer(() -> EPOCH_RESET + 1, Namespace.US_STREET_CASH_FUTURES.ordinal(), 1);

        String bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000000", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000001", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000010", bits);
    }

    //todo: test overflow
    @Test
    void parseTest() {
        long currentTimeMillis = System.currentTimeMillis();
        String formattedTime = DateTimeFormatter.ofPattern(TIME_PATTERN).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMillis), ZONE_OFFSET));

        Sequencer sequencer = new Sequencer(() -> currentTimeMillis, Namespace.US_STREET_CASH_FUTURES.ordinal(), 3);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"US_STREET_CASH_FUTURES\",partition:\"3\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));

        sequencer = new Sequencer(() -> currentTimeMillis, Namespace.US_STREET_CASH_EQUITY.ordinal(), 2);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"US_STREET_CASH_EQUITY\",partition:\"2\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"US_STREET_CASH_EQUITY\",partition:\"2\", sequence:\"1\"}", Sequencer.parse(sequencer.getNext()));
    }
}
