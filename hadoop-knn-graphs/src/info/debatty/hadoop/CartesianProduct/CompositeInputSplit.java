package info.debatty.hadoop.CartesianProduct;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author tibo
 */
public class CompositeInputSplit extends InputSplit implements Writable {

    private int fill = 0;
    private long totsize = 0L;
    private InputSplit[] splits;
    private Configuration conf = new Configuration();

    public CompositeInputSplit() {
    }

    public CompositeInputSplit(int capacity) {
        splits = new InputSplit[capacity];
    }

    /**
     * Add an InputSplit to this collection.
     *
     * @param s
     * @throws IOException If capacity was not specified during construction or
     * if capacity has been reached.
     * @throws java.lang.InterruptedException
     */
    public void add(InputSplit s) throws IOException, InterruptedException {
        if (null == splits) {
            throw new IOException("Uninitialized InputSplit");
        }
        if (fill == splits.length) {
            throw new IOException("Too many splits");
        }
        splits[fill++] = s;
        totsize += s.getLength();
    }

    /**
     * Get ith child InputSplit.
     * @param i
     * @return 
     */
    public InputSplit get(int i) {
        return splits[i];
    }

    /**
     * Return the aggregate length of all child InputSplits currently added.
     * @return 
     * @throws java.io.IOException
     */
    @Override
    public long getLength() throws IOException {
        return totsize;
    }

    /**
     * Get the length of ith child InputSplit.
     * @param i
     * @return 
     * @throws java.io.IOException 
     * @throws java.lang.InterruptedException 
     */
    public long getLength(int i) throws IOException, InterruptedException {
        return splits[i].getLength();
    }

    /**
     * Collect a set of hosts from all child InputSplits.
     * @return 
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        HashSet<String> hosts = new HashSet<String>();
        for (InputSplit s : splits) {
            String[] hints = s.getLocations();
            if (hints != null && hints.length > 0) {
                for (String host : hints) {
                    hosts.add(host);
                }
            }
        }
        return hosts.toArray(new String[hosts.size()]);
    }

    /**
     * getLocations from ith InputSplit.
     * @param i
     * @return 
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    public String[] getLocation(int i) throws IOException, InterruptedException {
        return splits[i].getLocations();
    }

    /**
     * Write splits in the following format.      {@code
     * <count><class1><class2>...<classn><split1><split2>...<splitn>
     * }
     * @param out
     * @throws java.io.IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, splits.length);
        for (InputSplit s : splits) {
            Text.writeString(out, s.getClass().getName());
        }
        for (InputSplit s : splits) {
            SerializationFactory factory = new SerializationFactory(conf);
            Serializer serializer = factory.getSerializer(s.getClass());
            serializer.open((DataOutputStream) out);
            serializer.serialize(s);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param in
     * @throws IOException If the child InputSplit cannot be read, typically for
     * failing access checks.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int card = WritableUtils.readVInt(in);
        if (splits == null || splits.length != card) {
            splits = new InputSplit[card];
        }
        Class<? extends InputSplit>[] cls = new Class[card];
        try {
            for (int i = 0; i < card; ++i) {
                cls[i]
                        = Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
            }
            for (int i = 0; i < card; ++i) {
                splits[i] = ReflectionUtils.newInstance(cls[i], null);
                SerializationFactory factory = new SerializationFactory(conf);
                Deserializer deserializer = factory.getDeserializer(cls[i]);
                deserializer.open((DataInputStream) in);
                splits[i] = (InputSplit) deserializer.deserialize(splits[i]);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed split init", e);
        }
    }

}
