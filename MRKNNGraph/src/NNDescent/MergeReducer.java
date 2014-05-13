package NNDescent;

import MRKNNGraph.Node;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author tibo
 */
class MergeReducer extends Reducer<Node, Neighbor, Node, NeighborList> {

    @Override
    protected void reduce(Node key, Iterable<Neighbor> values, Context context)
            throws IOException, InterruptedException {
        
        
        NeighborList nl = new NeighborList();
        for (Neighbor n : values) {
            if (!nl.contains(n)) {
                // Make a copy of n (as n will be reused!)
                nl.add(new Neighbor(n));
            }
        }
        
        context.write(key, nl);
    }
    
}
