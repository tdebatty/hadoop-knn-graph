package info.debatty.hadoop.graphs;

import info.debatty.graphs.Node;
import info.debatty.hadoop.util.ObjectWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A wrapper class that makes a node writable comparable
 * so that it can be used as a key or value by Hadoop
 * @author tibo
 */
public class NodeWritable implements WritableComparable {
    
    public Node node;
    
    public NodeWritable() {
        this.node = new Node();
    }
    
    /**
     * 
     * @param node
     */
    public NodeWritable(Node node) {
        this.node = node;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(node.id);
        
        ObjectWritable oj = new ObjectWritable(node.value);
        oj.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.node = new Node();
        node.id = in.readUTF();    
        
        try {
            node.value = ObjectWritable.readObject(in);
            
        } catch (Exception ex) {
            Logger.getLogger(NodeWritable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int compareTo(Object o) {
        if (! o.getClass().isInstance(this)) {
            throw new InvalidParameterException();
        }

        return this.node.id.compareTo(((NodeWritable) o).node.id);
    }
    
    @Override
    public boolean equals(Object o) {
        if (! o.getClass().isInstance(this)) {
            return false;
        }
        
        return this.node.equals(((NodeWritable) o).node);
        
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + (this.node != null ? this.node.hashCode() : 0);
        return hash;
    }
    
    /**
     * Returns the contained node
     * @return 
     */
    public Node get() {
        return this.node;
    }
    
    @Override
    public String toString() {
        return this.node.toString();
    }
}