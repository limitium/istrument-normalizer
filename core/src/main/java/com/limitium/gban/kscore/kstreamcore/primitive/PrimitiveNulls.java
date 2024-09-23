package com.limitium.gban.kscore.kstreamcore.primitive;

public class PrimitiveNulls {
    public static double NULL_DOUBLE = -Double.MAX_VALUE;
    public static float NULL_FLOAT = -Float.MAX_VALUE;
    public static int NULL_INT = Integer.MIN_VALUE;
    public static long NULL_LONG = Long.MIN_VALUE;
    public static short NULL_SHORT = Short.MIN_VALUE;
    public static char NULL_CHAR = (char) 0;
    public static byte NULL_BYTE = Byte.MIN_VALUE;

    public static boolean isNull(double value) {
        return value == NULL_DOUBLE;
    }

    public static boolean isNull(float value) {
        return value == NULL_FLOAT;
    }

    public static boolean isNull(int value) {
        return value == NULL_INT;
    }

    public static boolean isNull(long value) {
        return value == NULL_LONG;
    }

    public static boolean isNull(short value) {
        return value == NULL_SHORT;
    }

    public static boolean isNull(char value) {
        return value == NULL_CHAR;
    }

    public static boolean isNull(byte value) {
        return value == NULL_BYTE;
    }

    public static boolean notNull(double value) {
        return !isNull(value);
    }

    public static boolean notNull(float value) {
        return !isNull(value);
    }

    public static boolean notNull(int value) {
        return !isNull(value);
    }

    public static boolean notNull(long value) {
        return !isNull(value);
    }

    public static boolean notNull(short value) {
        return !isNull(value);
    }

    public static boolean notNull(char value) {
        return !isNull(value);
    }

    public static boolean notNull(byte value) {
        return !isNull(value);
    }

    public static double primitiveFrom(Double value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_DOUBLE;
    }

    public static float primitiveFrom(Float value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_FLOAT;
    }

    public static int primitiveFrom(Integer value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_INT;
    }

    public static long primitiveFrom(Long value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_LONG;
    }

    public static short primitiveFrom(Short value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_SHORT;
    }

    public static char primitiveFrom(Character value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_CHAR;
    }

    public static byte primitiveFrom(Byte value) {
        if (value != null) {
            return value;
        }
        return PrimitiveNulls.NULL_BYTE;
    }

    public static double withDefault(double value, double defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static float withDefault(float value, float defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static char withDefault(char value, char defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static byte withDefault(byte value, byte defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static short withDefault(short value, short defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static int withDefault(int value, int defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }

    public static long withDefault(long value, long defaultValue) {
        return !isNull(value) ? value : defaultValue;
    }
}
