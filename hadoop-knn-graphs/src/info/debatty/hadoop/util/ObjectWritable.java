/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package info.debatty.hadoop.util;

import java.lang.reflect.Array;
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 * A polymorphic Writable that writes an instance with it's class name. Handles
 * arrays, strings and primitive types without a Writable wrapper.
 */
public class ObjectWritable implements Writable {

    private Object instance;

    public ObjectWritable() {
    }

    /**
     *
     * @param instance
     */
    public ObjectWritable(Object instance) {
        this.instance = instance;
    }

    /**
     * Return the instance, or null if none.
     * @return 
     */
    public Object get() {
        return instance;
    }

    /**
     * Reset the instance.
     * @param instance
     */
    public void set(Object instance) {
        this.instance = instance;
    }

    @Override
    public String toString() {
        return "OW[value=" + instance + "]";
    }

    /**
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            this.instance = readObject(in);
        } catch (InstantiationException ex) {
            Logger.getLogger(ObjectWritable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(ObjectWritable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeObject(out, instance);
    }
    
    private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
    static {
        PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
        PRIMITIVE_NAMES.put("byte", Byte.TYPE);
        PRIMITIVE_NAMES.put("char", Character.TYPE);
        PRIMITIVE_NAMES.put("short", Short.TYPE);
        PRIMITIVE_NAMES.put("int", Integer.TYPE);
        PRIMITIVE_NAMES.put("long", Long.TYPE);
        PRIMITIVE_NAMES.put("float", Float.TYPE);
        PRIMITIVE_NAMES.put("double", Double.TYPE);
        PRIMITIVE_NAMES.put("void", Void.TYPE);
    }
    
    public static Object readObject(DataInput in) throws IOException, InstantiationException, IllegalAccessException {
        String className = in.readUTF();
        
        Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
        
        if (declaredClass == null) {
            try {
                declaredClass = Class.forName(className);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(ObjectWritable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        return readObject(in, declaredClass);
    }

    public static Object readObject(DataInput in, Class<?> declaredClass) throws IOException, InstantiationException, IllegalAccessException {
        Object instance;
        
        if (declaredClass.isPrimitive()) {            // primitive types
            if (declaredClass == Boolean.TYPE) {             // boolean
                instance = Boolean.valueOf(in.readBoolean());
            } else if (declaredClass == Character.TYPE) {    // char
                instance = Character.valueOf(in.readChar());
            } else if (declaredClass == Byte.TYPE) {         // byte
                instance = Byte.valueOf(in.readByte());
            } else if (declaredClass == Short.TYPE) {        // short
                instance = Short.valueOf(in.readShort());
            } else if (declaredClass == Integer.TYPE) {      // int
                instance = Integer.valueOf(in.readInt());
            } else if (declaredClass == Long.TYPE) {         // long
                instance = Long.valueOf(in.readLong());
            } else if (declaredClass == Float.TYPE) {        // float
                instance = Float.valueOf(in.readFloat());
            } else if (declaredClass == Double.TYPE) {       // double
                instance = Double.valueOf(in.readDouble());
            } else if (declaredClass == Void.TYPE) {         // void
                instance = null;
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }
            
        } else if (declaredClass.isArray()) {              // array
            int length = in.readInt();
            System.out.println("Will now read " + length + " elements from array");
            instance = Array.newInstance(declaredClass.getComponentType(), length);
            for (int i = 0; i < length; i++) {
                Array.set(instance, i, readObject(in, declaredClass.getComponentType()));
            }

        } else if (declaredClass == String.class) {        // String
            instance = in.readUTF();
            
        } else if (declaredClass.isEnum()) {         // enum
            instance = Enum.valueOf((Class<? extends Enum>) declaredClass, in.readUTF());
            
        } else {
            // Writable
            
            instance = declaredClass.newInstance();
            ((Writable) instance).readFields(in);
            
            //Class instanceClass = null;
            //String str = in.readUTF();
            //instanceClass = loadClass(conf, str);

            //Writable writable = WritableFactories.newInstance(instanceClass, conf);
            //writable.readFields(in);
            //instance = writable;
            //if (instanceClass == NullInstance.class) {  // null
            //    declaredClass = ((NullInstance) instance).declaredClass;
            //    instance = null;
            //}
            
            //instance = null;
        }
        
        return instance;
    }

    private static class NullInstance implements Writable {

        private Class<?> declaredClass;

        public NullInstance() {
        }

        public NullInstance(Class declaredClass) {
            this.declaredClass = declaredClass;
        }

        public void readFields(DataInput in) throws IOException {
            //String className = UTF8.readString(in);
            //declaredClass = PRIMITIVE_NAMES.get(className);
            if (declaredClass == null) {
                
                    //declaredClass = getConf().getClassByName(className);
                
            }
        }

        public void write(DataOutput out) throws IOException {
            //UTF8.writeString(out, declaredClass.getName());
        }
    }

    public static void writeObject(DataOutput out, Object instance) throws IOException {
        writeObject(out, instance, true);
    }
    
    /**
     * Write a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.
     * @param out
     * @param instance
     * @param write_type
     * @throws java.io.IOException
     */
    public static void writeObject(DataOutput out, Object instance, boolean write_type)
            throws IOException {
        
        if (instance == null) {                       // null
            //instance = new NullInstance(declaredClass);
            //declaredClass = Writable.class;
        }
        
        Class<?> declaredClass = instance.getClass();
        
        // Write declared class name
        if (write_type) {
            out.writeUTF(declaredClass.getName());
        }
        
        if (declaredClass.isArray()) {                // array
            int length = Array.getLength(instance);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeObject(out, Array.get(instance, i), false);
            }

        } else if (declaredClass == String.class) {   // String
            out.writeUTF((String) instance);

        } else if (declaredClass.isPrimitive()) {     // primitive type
            if (declaredClass == Boolean.TYPE) {        // boolean
                out.writeBoolean(((Boolean) instance).booleanValue());
            } else if (declaredClass == Character.TYPE) { // char
                out.writeChar(((Character) instance).charValue());
            } else if (declaredClass == Byte.TYPE) {    // byte
                out.writeByte(((Byte) instance).byteValue());
            } else if (declaredClass == Short.TYPE) {   // short
                out.writeShort(((Short) instance).shortValue());
            } else if (declaredClass == Integer.TYPE) { // int
                out.writeInt(((Integer) instance).intValue());
            } else if (declaredClass == Long.TYPE) {    // long
                out.writeLong(((Long) instance).longValue());
            } else if (declaredClass == Float.TYPE) {   // float
                out.writeFloat(((Float) instance).floatValue());
            } else if (declaredClass == Double.TYPE) {  // double
                out.writeDouble(((Double) instance).doubleValue());
            } else if (declaredClass == Void.TYPE) {    // void
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }
            
        } else if (declaredClass.isEnum()) {         // enum
            out.writeUTF(((Enum) instance).name());
            
        } else {
            // Should be a writable
            ((Writable) instance).write(out);
        }
    }
}
