package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.Node;
import info.debatty.hadoop.graphs.NeighborWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class MergeMapper extends Mapper<LongWritable, Text, NodeWritable, NeighborWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        try {
            // Input should look like
            // Node \tab Neighbor
            String[] input = value.toString().split("\t", 2);
            Node n = Node.parseString(input[0]);
            Neighbor neighbor = Neighbor.parseString(input[1]);
            context.write(
                    new NodeWritable(n),
                    new NeighborWritable(neighbor));
            
        } catch (Exception ex) {
            System.out.println("Could not parse : " + value.toString());
            context.getCounter("NNDescent", "Failed parsing").increment(1);
        }
    }
}

