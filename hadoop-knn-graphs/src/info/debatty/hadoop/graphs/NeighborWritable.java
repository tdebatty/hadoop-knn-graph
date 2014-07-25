package info.debatty.hadoop.graphs;

import info.debatty.graphs.Neighbor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.InvalidParameterException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author tibo
 */
public class NeighborWritable implements WritableComparable {
    
    public Neighbor neighbor;
    
    public NeighborWritable() {
        this.neighbor = new Neighbor();
    }
    
    public NeighborWritable(Neighbor neighbor) {
        this.neighbor = neighbor;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        NodeWritable wn = new NodeWritable(this.neighbor.node);
        wn.write(out);
        out.writeDouble(this.neighbor.similarity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.neighbor = new Neighbor();
        NodeWritable wn = new NodeWritable();
        wn.readFields(in);
        this.neighbor.node = wn.get();
        this.neighbor.similarity = in.readDouble();
    }

    @Override
    public int compareTo(Object other) {
        if (! other.getClass().isInstance(this)) {
            throw new InvalidParameterException();
        }
        
        return this.neighbor.compareTo( ((NeighborWritable) other).neighbor);
    }

    public Neighbor get() {
        return this.neighbor;
    }
    
    @Override
    public String toString() {
        return this.neighbor.toString();
    }
}
