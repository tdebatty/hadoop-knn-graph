package MRKNNGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author tibo
 */
public class Edge implements Writable, Comparable<Edge>{
    public Node n1;
    public Node n2;
    public double similarity = 0;
    
    public static final String SEPARATOR = ";";
    
    public Edge() {
        
    }

    public Edge(Node n1, Node n2, double similarity) {
        this.n1 = n1;
        this.n2 = n2;
        this.similarity = similarity;
    }

    public Edge(Edge e) {
        this.n1 = new Node(e.n1);
        this.n2 = new Node(e.n2);
        this.similarity = e.similarity;
    }
    
    @Override
    public String toString() {
        return n1.id + SEPARATOR + n2.id + SEPARATOR + similarity;
        
    }
    
    public static Edge parseString(String line) {
        String[] values = line.split(SEPARATOR);
        
        Edge e = new Edge();
        e.n1 = new Node(values[0]);
        e.n2 = new Node(values[1]);
        e.similarity = Double.valueOf(values[2]);
        
        return e;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(n1.id);
        d.writeUTF(n2.id);
        d.writeDouble(similarity);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.n1 = new Node(di.readUTF());
        this.n2 = new Node(di.readUTF());
        this.similarity = di.readDouble();
    }

    @Override
    public int compareTo(Edge e) {
        if (this.similarity < e.similarity) {
            return -1;
        } else if (this.similarity > e.similarity) {
            return 1;
        }
        
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        
        Edge other = (Edge) obj;        
        return n1.id.equals(other.n1.id) &&
                n2.id.equals(other.n2.id);
        
    } 

}
