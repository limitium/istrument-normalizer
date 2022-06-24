package extb.gba.sequencer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;

import static extb.gba.sequencer.Sequencer.EPOCH_RESET;

class SequencerTest {

    @Test
    void basicTest() {
        Sequencer sequencer = new Sequencer(() -> EPOCH_RESET + 1, Namespace.SPAN, 1);

        String bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000000", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000001", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000010000010000000010", bits);
    }
    //todo: test overflow
}
