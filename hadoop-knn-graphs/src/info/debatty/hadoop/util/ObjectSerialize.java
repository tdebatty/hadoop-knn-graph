package info.debatty.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author tibo
 */
public class ObjectSerialize {

    /**
     * Read the object from Base64 string.
     *
     * @param s
     * @return
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public static Object unserialize(String s)
            throws IOException, ClassNotFoundException {
        byte[] data = Base64.decodeBase64(s);

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        
        return ois.readObject();
    }

    /**
     * Write the object to a Base64 string.
     *
     * @param o
     * @return
     * @throws java.io.IOException
     */
    public static String serialize(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        
        return new String(Base64.encodeBase64(baos.toByteArray()));
    }
}
