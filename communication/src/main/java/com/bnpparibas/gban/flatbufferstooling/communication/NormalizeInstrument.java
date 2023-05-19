package com.bnpparibas.gban.flatbufferstooling.communication;

import com.bnpparibas.gban.communication.messages.internal.instrumentnormalizer.receiver.flatbuffers.FBNormalizeInstrument;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import org.apache.bcel.Repository;
import org.apache.bcel.classfile.ConstantPool;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
     *              ".upstream.order", // path from the root where instrument id should be injected.
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
     * @param pathToInstrumentTable comma separated path of getters to table with instrument mutator, starting from root `.`
     * @param egressTopic           topic which will be used for publishing business message with normalized instrument
     * @return {@link FBNormalizeInstrument} sized table
     */
    public static FBNormalizeInstrument wrapWithNormalizeInstrument(Table fbTable, long msgId, String securityId, String pathToInstrumentTable, String egressTopic) {
        assertMutateMethod(fbTable, pathToInstrumentTable);

        FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder().forceDefaults(true);

        int securityIdOff = flatBufferBuilder.createString(securityId);
        int egressTopicOff = flatBufferBuilder.createString(egressTopic);
        int instrumentIdOffset = getInstrumentIdOffset(fbTable, pathToInstrumentTable);
        int msgClazzOff = flatBufferBuilder.createString(fbTable.getClass().getName());
        int msgDataOff = flatBufferBuilder.createByteVector(fbTable.getByteBuffer().array());

        flatBufferBuilder.finish(FBNormalizeInstrument.createFBNormalizeInstrument(flatBufferBuilder, securityIdOff, msgClazzOff, msgDataOff, msgId, instrumentIdOffset, egressTopicOff));

        return FBNormalizeInstrument.getRootAsFBNormalizeInstrument(ByteBuffer.wrap(flatBufferBuilder.sizedByteArray()));
    }

    private static int getInstrumentIdOffset(Table originalMessage, String pathToInstrumentTable) {
        Table instrumentTable = (Table) findInstrumentTable(originalMessage, pathToInstrumentTable);
        return getOffsetForField(instrumentTable);
    }

    static Map<Class<? extends Table>, CachedOffsetMeta> cachedOffset = new HashMap<>();

    static class CachedOffsetMeta {
        //@todo: move to VH if performance lack
        Field bbPosField;
        java.lang.reflect.Method offsetMethod;
        int fieldRefOffset;

        public CachedOffsetMeta(Field bbPosField, Method offsetMethod, int fieldRefOffset) {
            this.bbPosField = bbPosField;
            this.offsetMethod = offsetMethod;
            this.fieldRefOffset = fieldRefOffset;
        }

        int getOffset(Table table) throws IllegalAccessException, InvocationTargetException {
            Integer bbPos = (Integer) bbPosField.get(table);
            return bbPos + (int) offsetMethod.invoke(table, fieldRefOffset);
        }
    }

    private static int getOffsetForField(Table table) {
        try {
            if (!cachedOffset.containsKey(table.getClass())) {
                //VTable offset in bb
                Field bbPosField = table.getClass().getSuperclass().getDeclaredField("bb_pos");
                bbPosField.setAccessible(true);

                //Field reference offset in vtable
                int fieldOffset = lookupFieldOffsetInVtable(table.getClass(), MUTATE_INSTRUMENT_ID);

                //Field value offset in vtable
                java.lang.reflect.Method offsetMethod = table.getClass().getSuperclass().getDeclaredMethod("__offset", int.class);
                offsetMethod.setAccessible(true);

                cachedOffset.put(table.getClass(), new CachedOffsetMeta(bbPosField, offsetMethod, fieldOffset));
            }

            return cachedOffset.get(table.getClass()).getOffset(table);
        } catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static int lookupFieldOffsetInVtable(Class<? extends Table> tableClass, String mutatorName) throws ClassNotFoundException {
        JavaClass jc = Repository.lookupClass(tableClass.getName());

        ConstantPool constantPool = jc.getConstantPool();
        org.apache.bcel.classfile.Method[] method = jc.getMethods();
        ConstantPoolGen cpg = new ConstantPoolGen(constantPool);

        for (org.apache.bcel.classfile.Method m : method) {
            if (m.getName().equalsIgnoreCase(mutatorName)) {
                MethodGen mg = new MethodGen(m, m.getName(), cpg);
                for (InstructionHandle ih = mg.getInstructionList().getStart();
                     ih != null; ih = ih.getNext()) {
                    if (ih.getPosition() == 1) {
                        return extractOffsetFromInstruction(tableClass, mutatorName, ih.getInstruction());
                    }
                }
            }
        }
        throw new RuntimeException("Offset wasn't found in " + tableClass + "#" + mutatorName);
    }

    private static int extractOffsetFromInstruction(Class<? extends Table> tableClass, String mutatorName, Instruction instruction) {
        if (instruction instanceof BIPUSH bipush) {
            return bipush.getValue().intValue();
        } else if (instruction instanceof ICONST iconst) {
            return iconst.getValue().intValue();
        }
        throw new RuntimeException("Unsupported 1st opcode" + instruction + "in " + tableClass + "#" + mutatorName);
    }

    /**
     * Mutates instrument id in a table of the original message according to {@link FBNormalizeInstrument#instrumentIdOffest()} ()}
     *
     * @param normalizeInstrument request with original flatbuffers message to be mutated
     * @param instrumentId        public instrument id to be set in the original message
     */
    public static void mutateInstrumentId(FBNormalizeInstrument normalizeInstrument, long instrumentId) {
        ByteBuffer byteBuffer = normalizeInstrument.originalMessageAsByteBuffer();
        byteBuffer.putLong(byteBuffer.position() + normalizeInstrument.instrumentIdOffest(), instrumentId);
    }

    /**
     * Cuts sized byte array with an original message content from {@link FBNormalizeInstrument}
     *
     * @param normalizeInstrument wrapper around business message
     * @return sized byte array of original business message
     */
    public static byte[] cutOriginalMessageFrom(FBNormalizeInstrument normalizeInstrument) {
        byte[] originalMessageBytes = new byte[normalizeInstrument.originalMessageLength()];
        normalizeInstrument.originalMessageAsByteBuffer()
                .get(originalMessageBytes,
                        0,
                        originalMessageBytes.length);
        return originalMessageBytes;
    }

    private static void assertMutateMethod(Table fbTable, String pathToInstrumentTable) {
        Object instrumentTable = findInstrumentTable(fbTable, pathToInstrumentTable);
        //@todo: check that original value isn't default
        findMutator(instrumentTable);
    }

    private static Object findInstrumentTable(Table fbTable, String pathToInstrumentTable) {
        Object instrumentTable = fbTable;
        if (".".equalsIgnoreCase(pathToInstrumentTable)) {
            return instrumentTable;
        }

        if (pathToInstrumentTable != null) {
            String[] pathParts = pathToInstrumentTable.split("\\.");

            if (pathParts.length < 1) {
                throw new RuntimeException("Empty path to instrument table");
            }

            String[] gettersName = Arrays.copyOfRange(pathParts, 1, pathParts.length);
            for (String getterName : gettersName) {
                try {
                    Method getter = instrumentTable.getClass().getMethod(getterName);
                    instrumentTable = getter.invoke(instrumentTable);
                } catch (NoSuchMethodException e) {
                    throw new NormalizeInstrumentException("Getter method " + getterName + " not found in table " + instrumentTable.getClass(), e);
                } catch (InvocationTargetException | IllegalAccessException e) {
                    throw new NormalizeInstrumentException(e);
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
            throw new NormalizeInstrumentException("Missing `mutateInstrumentId(long)` method in table " + instrumentTable.getClass(), e);
        }
    }

    public static class NormalizeInstrumentException extends RuntimeException {

        public NormalizeInstrumentException(Exception exception) {
            super(exception);
        }

        public NormalizeInstrumentException(String reason, Exception exception) {
            super(reason, exception);
        }

        public NormalizeInstrumentException(String reason) {
            super(reason);
        }
    }
}