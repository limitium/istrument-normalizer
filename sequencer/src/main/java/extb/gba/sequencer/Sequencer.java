package extb.gba.sequencer;


import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

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
    private static final int MILLIS_BITS = 40;
    private static final int PARTITION_BITS = 6;
    public static final long PARTITION_MASK = ~(-1L << PARTITION_BITS);
    private static final int SEQUENCE_BITS = 10;
    private static final int NAMESPACE_BITS = 7;
    public static final long NAMESPACE_MASK = ~(-1L << NAMESPACE_BITS);
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);
    private static final long MILLIS_MASK = ~(-1L << MILLIS_BITS);
    public static final String TIME_PATTERN = "yyyy:MM:dd-HH:mm:ss:SSS";
    public static final ZoneOffset ZONE_OFFSET = ZoneOffset.UTC;
    private final Clock clock;
    private final long sequencerBits;
    private long sequence = 0L;
    private long prevMillis;

    public Sequencer(Clock clock, Namespace namespace, int partition) {
        if (partition < 0 || partition > 63) {
            throw new IllegalArgumentException("Partition must be >= 0 and < 64, current value is " + partition);
        }
        if (namespace.ordinal() > 127) {
            throw new IllegalArgumentException("Sequencer type must be < 128, current value is " + namespace.ordinal());
        }

        this.clock = clock;

        this.sequencerBits = (NAMESPACE_MASK & namespace.ordinal()) << (PARTITION_BITS + SEQUENCE_BITS)
                | (partition & PARTITION_MASK) << SEQUENCE_BITS;
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
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                millis = waitForNextMillis(prevMillis);
            }
        } else {
            throw new RuntimeException("Clock went back");
        }

        prevMillis = millis;

        long millisBits = ((millis - EPOCH_RESET) & MILLIS_MASK) << (NAMESPACE_BITS + PARTITION_BITS + SEQUENCE_BITS);
        return millisBits | sequencerBits | sequence;
    }

    private long waitForNextMillis(long prevMillis) {
        long millis = clock.millis();
        while (millis == prevMillis) {
            millis = clock.millis();
        }
        return millis;
    }

    public static String parse(long sequence) {
        Instant epochMilli = Instant.ofEpochMilli((sequence >> (SEQUENCE_BITS + PARTITION_BITS + NAMESPACE_BITS) & MILLIS_MASK) + EPOCH_RESET);
        return "{time:\"" + DateTimeFormatter.ofPattern(TIME_PATTERN).format(ZonedDateTime.ofInstant(epochMilli, ZONE_OFFSET)) + "\"," +
                "namespace:\"" + (sequence >> (SEQUENCE_BITS + PARTITION_BITS) & NAMESPACE_MASK) + "\"," +
                "partition:\"" + (sequence >> SEQUENCE_BITS & PARTITION_MASK) + "\", " +
                "sequence:\"" + (sequence & SEQUENCE_MASK) + "\"}";
    }

    public static class SystemClock implements Clock {

        @Override
        public long millis() {
            return System.currentTimeMillis();
        }
    }
}
