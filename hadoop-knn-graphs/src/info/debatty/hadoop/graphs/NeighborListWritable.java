package info.debatty.hadoop.graphs;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.hadoop.util.ObjectWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author tibo
 */
public class NeighborListWritable implements Writable {
    
    public NeighborList nl;

    public NeighborListWritable() {
        this.nl = new NeighborList();
    }
    
    public NeighborListWritable(NeighborList nl) {
        this.nl = nl;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Convert to an array of WritableNeighbors
        NeighborWritable[] wns = new NeighborWritable[this.nl.size()];
        int i = 0;
        for (Neighbor n : this.nl) {
            wns[i++] = new NeighborWritable(n);
        }
        
        // Use ObjectWritable to write the array of writable
        ObjectWritable.writeObject(out, wns);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nl = new NeighborList();
        NeighborWritable[] array;
        
        try {
            array = (NeighborWritable[]) ObjectWritable.readObject(in);
            
        } catch (Exception ex) {
            Logger.getLogger(NeighborListWritable.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        this.nl = new NeighborList();
        for (NeighborWritable n : array) {
            this.nl.add(n.neighbor);
        }
    }

    public NeighborList get() {
        return this.nl;
    }
    
    @Override
    public String toString() {
        return this.nl.toString();
    }
}
