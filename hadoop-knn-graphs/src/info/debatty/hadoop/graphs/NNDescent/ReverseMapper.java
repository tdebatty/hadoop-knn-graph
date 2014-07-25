package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.graphs.Node;
import info.debatty.hadoop.graphs.NeighborWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class ReverseMapper extends Mapper<LongWritable, Text, NodeWritable, NeighborWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        try {
            // Input should look like
            // Node tab NeighborList
            String[] input = value.toString().split("\t", 2);
            Node n = Node.parseString(input[0]);
            NeighborList nl = NeighborList.parseString(input[1]);

            for (Neighbor neighbor : nl) {
                context.write(new NodeWritable(n), new NeighborWritable(neighbor));
                context.write(
                        new NodeWritable(neighbor.node),
                        new NeighborWritable(new Neighbor(n, neighbor.similarity)));
            }
            
        } catch (Exception ex) {
            System.out.println("Failed to parse " + value.toString());
            context.getCounter("NNDescent", "Failed parsing").increment(1);
        }
    }
    
    
}