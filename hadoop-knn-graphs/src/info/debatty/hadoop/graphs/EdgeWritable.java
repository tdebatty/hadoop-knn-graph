package info.debatty.hadoop.graphs;

import info.debatty.graphs.Node;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author tibo
 */
public class EdgeWritable extends info.debatty.graphs.Edge implements WritableComparable {
    
    public EdgeWritable() {
        
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(n1.id);
        out.writeUTF(n2.id);
        out.writeDouble(similarity);
        
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.n1 = new Node(in.readUTF());
        this.n2 = new Node(in.readUTF());
        this.similarity = in.readDouble();
    }


}
