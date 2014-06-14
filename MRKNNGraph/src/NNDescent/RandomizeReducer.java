package NNDescent;

import MRKNNGraph.Node;
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
        extends Reducer<LongWritable, Node, Node, Neighbor> {
        
        int K = 10;

        public RandomizeReducer() {
        }

 
        
        

    @Override
    protected void reduce(LongWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
                
            
            ArrayList<Node> nodes = new ArrayList<Node>();
            Random rnd = new Random();
            
            for (Node n : values) {
                nodes.add(new Node(n));
            }
            
            for (Node n : nodes) {
                Neighbor neigbor = new Neighbor(
                            nodes.get(rnd.nextInt(nodes.size())),
                            Double.MAX_VALUE);
                context.write(n, neigbor);
            }
        }
    }
