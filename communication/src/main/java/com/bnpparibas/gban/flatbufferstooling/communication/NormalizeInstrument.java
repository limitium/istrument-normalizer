package com.bnpparibas.gban.flatbufferstooling.communication;

import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Utility class for wrapping business flatbuffers messages, with normalize instrument cmd.
 */
public class NormalizeInstrument {

    public static final String MUTATE_INSTRUMENT_ID = "mutateInstrumentId";

    /**
     * Wraps flatbuffers with normalize instrument. Throws RuntimeException if {@code mutateInstrumentId(long)} is missed.
     * <p>
     * Usage example
     *
     * <pre>{@code
     *      FBNormalizeInstrument normalizeInstrument = NormalizeInstrument
     *          .wrapWithNormalizeInstrument(
     *              executionReport, //root flatbuffers object
     *              1234, //object public id, should be uniq
     *              "IBM.N", // security id to be normalized
     *              "upstream.order", // path from the root where instrument id should be injected.
     *              // will be resolved into call executionReport.upstream().order().mutateInstrumentId(instrumentId)
     *              "happy.path.topic" // topic where normalized message will be placed
     *          );
     *
     *     //publish normalizeInstrument to InstrumentNormalizer
     *
     * }</pre>
     *
     * @param fbTable               flatbuffers table for normalization
     * @param msgId                 uniq identifier of the message
     * @param securityId            alphanumeric non-normalized instrument identifier
     * @param pathToInstrumentTable comma separated path of getters to table with instrument mutator, starting from root .
     * @param egressTopic           topic which will be used for publishing business message with normalized instrument
     * @return {@code FBNormalizeInstrument} sized table
     */
    public static FBNormalizeInstrument wrapWithNormalizeInstrument(Table fbTable, long msgId, String securityId, String pathToInstrumentTable, String egressTopic) {
        assertMutateMethod(fbTable, pathToInstrumentTable);

        FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder().forceDefaults(true);

        int securityIdOff = flatBufferBuilder.createString(securityId);
        int egressTopicOff = flatBufferBuilder.createString(egressTopic);
        int pathToInstrumentTableOff = flatBufferBuilder.createString(pathToInstrumentTable);
        int msgClazzOff = flatBufferBuilder.createString(fbTable.getClass().getName());
        int msgDataOff = flatBufferBuilder.createByteVector(fbTable.getByteBuffer().array());

        flatBufferBuilder.finish(FBNormalizeInstrument.createFBNormalizeInstrument(flatBufferBuilder, securityIdOff, msgClazzOff, msgDataOff, msgId, pathToInstrumentTableOff, egressTopicOff));

        return FBNormalizeInstrument.getRootAsFBNormalizeInstrument(ByteBuffer.wrap(flatBufferBuilder.sizedByteArray()));
    }

    /**
     * Mutates instrument id in a table of the original message according to {@link FBNormalizeInstrument#pathToInstrumentTable()}
     *
     * @param normalizeInstrument request with original flatbuffers message to be mutated
     * @param instrumentId        public instrument id to be set in the original message
     */
    public static void mutateInstrumentId(FBNormalizeInstrument normalizeInstrument, long instrumentId) {
        try {
            Table originalMessage = getOriginalTable(normalizeInstrument);

            Object instrumentTable = findInstrumentTable(originalMessage, normalizeInstrument.pathToInstrumentTable());
            Method mutator = findMutator(instrumentTable);

            if (!(boolean) mutator.invoke(instrumentTable, instrumentId)) {
                throw new RuntimeException("Instrument wasn't mutated, probably it was set to default value and FlatBufferBuilder().forceDefaults(false) was used");
            }
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException("Unable to mutate instrument id", e);
        }
    }

    private static void assertMutateMethod(Table fbTable, String pathToInstrumentTable) {
        Object instrumentTable = findInstrumentTable(fbTable, pathToInstrumentTable);
        //@todo: check that original value isn't default
        findMutator(instrumentTable);
    }

    private static Object findInstrumentTable(Table fbTable, String pathToInstrumentTable) {
        Object instrumentTable = fbTable;

        if (pathToInstrumentTable != null) {
            String[] getterNames = pathToInstrumentTable.split("\\.");
            for (String getterName : getterNames) {
                try {
                    Method getter = instrumentTable.getClass().getMethod(getterName);
                    instrumentTable = getter.invoke(instrumentTable);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Getter method " + getterName + " not found in table " + instrumentTable.getClass(), e);
                } catch (InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (instrumentTable == null) {
            throw new RuntimeException("Instrument table is null");
        }
        return instrumentTable;
    }

    private static Method findMutator(Object instrumentTable) {
        try {
            return instrumentTable.getClass().getMethod(MUTATE_INSTRUMENT_ID, long.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Missing `mutateInstrumentId(long)` method in table " + instrumentTable.getClass(), e);
        }

    }

    private static Table getOriginalTable(FBNormalizeInstrument normalizeInstrument) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Class<?> clazz = normalizeInstrument.getClass().getClassLoader().loadClass(normalizeInstrument.originalMessageClass());
        Method rootMaker = clazz.getMethod("getRootAs" + clazz.getSimpleName(), ByteBuffer.class);

        return (Table) rootMaker.invoke(null, normalizeInstrument.originalMessageAsByteBuffer());
    }
}