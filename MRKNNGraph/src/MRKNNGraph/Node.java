package MRKNNGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author tibo
 */
public class Node implements WritableComparable<Node> {
    public Text id = new Text("");
    public String value = "";
    
    public static final String DELIMITER = "___";
    
    public Node() {
        
    }

    /**
     * Constructor creates a deep copy of Node other
     * 
     * @param other 
     */
    public Node(Node other) {
        // Create a copy of the Text object
        this.id = new Text(other.id.toString());
        this.value = other.value;
    }
    
    public Node(String id) {
        this.id = new Text(id);
    }


    @Override
    public void write(DataOutput d) throws IOException {
        id.write(d);
        d.writeUTF(value);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id.readFields(di);
        value = di.readUTF();
        
    }

    @Override
    public String toString() {
        return id.toString() + DELIMITER + value;
    }
    
    
    public static Node parseString(String string) {
        String[] values = string.split(DELIMITER, 2);
        Node n = new Node();
        n.id.set(values[0]);
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.id);
        return hash;
    }
    
    
}
