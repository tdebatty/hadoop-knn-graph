package NNDescent;

import MRKNNGraph.Node;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author tibo
 */
public class Neighbor implements WritableComparable<Neighbor> {


    public Node node;
    public double similarity;

    public static final String DELIMITER = ",,,";
    
    public Neighbor(Node node, double similarity) {
        this.node = node;
        this.similarity = similarity;
    }

    public Neighbor(Neighbor n) {
        this.node = new Node(n.node);
        this.similarity = n.similarity;
    }

    public Neighbor() {
        node = new Node();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        node.write(d);
        d.writeDouble(similarity);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        node.readFields(di);
        similarity = di.readDouble();
    }

    @Override
    public String toString() {
        return node.toString() + DELIMITER + String.valueOf(similarity);
    }
    
    public static Neighbor parseString(String s) {
        String[] values = s.split(DELIMITER, 2);
        return new Neighbor(Node.parseString(values[0]), Double.valueOf(values[1]));
        
    }

    @Override
    public boolean equals(Object other) {
        if (!other.getClass().getName().equals(this.getClass().getName())) {
            return false;
        }
        
        Neighbor other_neighbor = (Neighbor) other;
        return this.node.equals(other_neighbor.node);
        
    }
    
    

    @Override
    public int compareTo(Neighbor other) {
        //if (this.node.equals(other.node)) {
        //    return 0;
        //}
        
        if (this.similarity == other.similarity) {
            return 0;
        }
        
        return this.similarity > other.similarity ? 1 : -1;
    }
    
}
