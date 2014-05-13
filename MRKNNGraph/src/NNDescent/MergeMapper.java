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
class MergeMapper extends Mapper<LongWritable, Text, Node, Neighbor> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        // Input should look like
        // Node tab Neighbor
        String[] input = value.toString().split("\t", 2);
        Node n = Node.parseString(input[0]);
        Neighbor neighbor = Neighbor.parseString(input[1]);
        context.write(n, neighbor);

    }
    
    
}