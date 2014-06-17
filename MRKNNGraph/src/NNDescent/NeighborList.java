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
        neighbors = new PriorityQueue<Neighbor>();
    }

    /**
     *
     * @param other
     */
    public NeighborList(NeighborList other) {
        super(Neighbor.class);
        this.neighbors = new PriorityQueue<Neighbor>();
        for (Neighbor n : other.neighbors) {
            // create a copy of each neighbor in the other neighbor's list
            this.neighbors.add(new Neighbor(n));
        }
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
        neighbors = new PriorityQueue<Neighbor>();
        
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
        nl.MAX_SIZE = Integer.MAX_VALUE;
        for (String s : values) {
            try {
                nl.add(Neighbor.parseString(s));
            } catch (Exception ex) {
                System.out.println("Failed to parse " + string);
            }
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
        NeighborList copy = new NeighborList(other);
        int count = 0;
        for (Neighbor n : this.neighbors) {
            String this_value = n.node.value;
            
            for (Neighbor other_n : copy.neighbors) {
                if (other_n.node.value.equals(this_value)) {
                    count++;
                    copy.neighbors.remove(other_n);
                    break;
                }
            }
        }
        
        return count;
    }
}
