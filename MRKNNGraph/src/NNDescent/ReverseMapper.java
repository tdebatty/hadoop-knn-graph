package NNDescent;

import MRKNNGraph.Node;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author tibo
 */
class ReverseMapper extends Mapper<LongWritable, Text, Node, Neighbor> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // Input should look like
        // Node tab NeighborList
        String[] input = value.toString().split("\t", 2);
        Node n = Node.parseString(input[0]);
        NeighborList nl = NeighborList.parseString(input[1]);
        
        for (Neighbor neighbor : nl) {
            context.write(n, neighbor);
            context.write(neighbor.node, new Neighbor(n, neighbor.similarity));
        }
    }
    
    
}
