package MRKNNGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author tibo
 */
public class Node implements Writable {
    public String id;
    public String value;
    
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
    
}
