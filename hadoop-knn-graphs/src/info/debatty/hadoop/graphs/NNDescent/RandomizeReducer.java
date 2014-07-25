package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.Node;
import info.debatty.hadoop.graphs.NeighborWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author tibo
 */
class RandomizeReducer 
        extends Reducer<LongWritable, NodeWritable, NodeWritable, NeighborWritable> {
        
        int K = 10;

        public RandomizeReducer() {
        }

    @Override
    protected void reduce(LongWritable key, Iterable<NodeWritable> values, Context context) throws IOException, InterruptedException {
                
            
            ArrayList<Node> nodes = new ArrayList<Node>();
            Random rnd = new Random();
            
            for (NodeWritable n : values) {
                nodes.add(n.get());
            }
            
            for (Node n : nodes) {
                Neighbor neigbor = new Neighbor(
                            nodes.get(rnd.nextInt(nodes.size())),
                            Double.MAX_VALUE);
                context.write(
                        new NodeWritable(n),
                        new NeighborWritable(neigbor));
            }
        }
    }
