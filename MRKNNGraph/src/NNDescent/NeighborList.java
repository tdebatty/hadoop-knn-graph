package NNDescent;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author tibo
 */
public class NeighborList extends ArrayWritable implements Iterable<Neighbor> {
    public static final String DELIMITER = ";;;";
    public int MAX_SIZE = 10;

    PriorityQueue<Neighbor> neighbors;

    public NeighborList() {
        super(Neighbor.class);
        neighbors = new PriorityQueue<>();
    }

    public void add(Neighbor neighbor) {
        if (neighbors.contains(neighbor)) {
            return;
        }
        
        if (neighbors.size() < MAX_SIZE) {
            neighbors.add(neighbor);
            return;
        }
        
        if (neighbors.peek().similarity < neighbor.similarity) {
            neighbors.poll();
            neighbors.add(neighbor);
        }
        
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.set(neighbors.toArray(new Neighbor[neighbors.size()]));
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        neighbors = new PriorityQueue<>();
        
        Writable[] array = this.get();
        for (Writable element : array) {
            neighbors.add((Neighbor) element);
        }
    }

    @Override
    public String toString() {
        if (neighbors.isEmpty()) {
            return "";
        }
        
        StringBuilder builder = new StringBuilder();
        for (Neighbor n : neighbors) {
            builder.append(n.toString()).append(DELIMITER);
        }
        builder.delete(builder.length()-3, Integer.MAX_VALUE);
        
        return builder.toString();
    }
    
    public static NeighborList parseString(String string) {
        String[] values = string.split(DELIMITER);
        NeighborList nl = new NeighborList();
        for (String s : values) {
            nl.add(Neighbor.parseString(s));
        }
        return nl;
    }

    public boolean contains(Neighbor n) {
        return neighbors.contains(n);
    }

    @Override
    public Iterator<Neighbor> iterator() {
        return neighbors.iterator();
    }

    public int size() {
        return neighbors.size();
    }
    
    public int CountCommons(NeighborList other) {
        int count = 0;
        for (Neighbor n : this) {
            if (other.contains(n)) {
                count++;
            }
        }
        
        return count;
    }
    
}
