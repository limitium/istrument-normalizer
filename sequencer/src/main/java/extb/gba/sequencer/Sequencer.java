package extb.gba.sequencer;


/**
 * Sequencer layout for 64bit long storage.
 * <p>
 * 0_0000000000000000000000000000000000000000_0000000_000000_0000000000
 * ^1 bit, always 0 for positive values
 * __^40 bit, 2^40/(1000*60*60*24*365) = 34 years in millis, from epoch reset
 * ___________________________________________^7 bit, 128 sequencer types
 * ___________________________________________________^6 bit, 64 partition
 * _________________________________________________________^10 bit, 1024 counter per ms,
 */

public class Sequencer {
    /**
     * Sun May 22 2022 11:46:40
     */
    public static final long EPOCH_RESET = 1653220000000L;
    public static final int MILLIS_BITS = 40;
    public static final int PARTITION_BITS = 6;
    public static final int SEQUENCE_BITS = 10;
    public static final int SEQUENCER_TYPE_BITS = 7;

    final Clock clock;
    private final long sequenceMask = ~(-1L << SEQUENCE_BITS);
    private final long millisMask = ~(-1L << MILLIS_BITS);
    private long sequence = 0L;
    private long prevMillis;
    private final long sequencerBits;

    public Sequencer(Clock clock, SequencerType sequencerType, int partition) {
        if (partition < 0 || partition > 63) {
            throw new IllegalArgumentException("Partition must be >= 0 and < 64, current value is " + partition);
        }
        if (sequencerType.ordinal() > 127) {
            throw new IllegalArgumentException("Sequencer type must be < 128, current value is " + sequencerType.ordinal());
        }

        this.clock = clock;

        long sequencerTypeMask = ~(-1L << SEQUENCER_TYPE_BITS);

        long partitionMask = ~(-1L << PARTITION_BITS);

        this.sequencerBits = (sequencerTypeMask & sequencerType.ordinal()) << (PARTITION_BITS + SEQUENCE_BITS)
                | (partition & partitionMask) << SEQUENCE_BITS;
    }

    /**
     * Non thread safe
     *
     * @return
     */
    public long getNext() {
        long millis = clock.millis();

        if (millis > prevMillis) {
            sequence = 0L;
        } else if (millis == prevMillis) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                millis = waitForNextMillis(prevMillis);
            }
        } else {
            throw new RuntimeException("Clock went back");
        }

        prevMillis = millis;

        long millisBits = ((millis - EPOCH_RESET) & millisMask) << (SEQUENCER_TYPE_BITS + PARTITION_BITS + SEQUENCE_BITS);
        return millisBits | sequencerBits | sequence;
    }

    private long waitForNextMillis(long prevMillis) {
        long millis = clock.millis();
        while (millis == prevMillis) {
            millis = clock.millis();
        }
        return millis;
    }

    public static class SystemClock implements Clock {

        @Override
        public long millis() {
            return System.currentTimeMillis();
        }
    }
}
