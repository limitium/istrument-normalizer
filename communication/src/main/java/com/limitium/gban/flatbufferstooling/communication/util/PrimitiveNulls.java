package com.limitium.gban.flatbufferstooling.communication.util;

/**
 * PrimitiveNulls utils, consider {@code import static com.db.axcore.utils.PrimitiveNulls.*;}
 */
public final class PrimitiveNulls {
    public static double NULL_DOUBLE = -Double.MAX_VALUE;
    public static float NULL_FLOAT = -Float.MAX_VALUE;
    public static int NULL_INT = Integer.MIN_VALUE;
    public static long NULL_LONG = Long.MIN_VALUE;
    public static short NULL_SHORT = Short.MIN_VALUE;
    public static char NULL_CHAR = (char)0;
    public static byte NULL_BYTE = Byte.MIN_VALUE;

    public static boolean isNull(double value) { return value==NULL_DOUBLE; }
    public static boolean isNull(float value) { return value==NULL_FLOAT; }
    public static boolean isNull(int value) { return value==NULL_INT; }
    public static boolean isNull(long value) { return value==NULL_LONG; }
    public static boolean isNull(short value) { return value==NULL_SHORT; }
    public static boolean isNull(char value) { return value==NULL_CHAR; }
    public static boolean isNull(byte value) { return value==NULL_BYTE; }

    public static boolean notNull(double value) { return !isNull(value); }
    public static boolean notNull(float value) { return !isNull(value); }
    public static boolean notNull(int value) { return !isNull(value); }
    public static boolean notNull(long value) { return !isNull(value); }
    public static boolean notNull(short value) { return !isNull(value); }
    public static boolean notNull(char value) { return !isNull(value); }
    public static boolean notNull(byte value) { return !isNull(value); }


    public static double nvl(double value, double defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static float nvl(float value, float defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static char nvl(char value, char defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static byte nvl(byte value, byte defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static short nvl(short value, short defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static int nvl(int value, int defaultValue) { return !isNull(value) ? value : defaultValue;}
    public static long nvl(long value, long defaultValue) { return !isNull(value) ? value : defaultValue;}

    public static double mul(double x, double y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x*y : defaultValue;}
    public static float mul(float x, float y, float defaultValue) { return !(isNull(x) || isNull(y)) ? x*y : defaultValue;}
    public static double mul(double x, int y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x*y : defaultValue;}
    public static double mul(double x, long y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x*y : defaultValue;}

    public static double add(double x, double y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x+y : defaultValue;}

    public static double sub(double x, double y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x-y : defaultValue;}

    public static double div(double x, double y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x/y : defaultValue;}
    public static double div(double x, int y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x/y : defaultValue;}
    public static double div(double x, long y, double defaultValue) { return !(isNull(x) || isNull(y)) ? x/y : defaultValue;}
}