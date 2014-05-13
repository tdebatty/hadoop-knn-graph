package MRKNNGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author tibo
 */
public class Node implements WritableComparable<Node> {
    public String id = "";
    public String value = "";
    
    public static final String DELIMITER = "___";
    
    public Node() {
        
    }

    public Node(Node n) {
        this.id = n.id;
        this.value = n.value;
    }
    
    public Node(String id) {
        this.id = id;
    }


    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(id);
        d.writeUTF(value);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id = di.readUTF();
        value = di.readUTF();
        
    }

    @Override
    public String toString() {
        return id + DELIMITER + value;
    }
    
    
    public static Node parseString(String string) {
        String[] values = string.split(DELIMITER, 2);
        Node n = new Node();
        n.id = values[0];
        n.value = values[1];
        return n;
    }

    @Override
    public int compareTo(Node other) {
        return id.compareTo(other.id);
    }

    @Override
    public boolean equals(Object other) {
        if (!other.getClass().getName().equals(this.getClass().getName())) {
            return false;
        }
        
        Node other_node = (Node) other;
        return other_node.id.equals(this.id);
    }
    
    
}
