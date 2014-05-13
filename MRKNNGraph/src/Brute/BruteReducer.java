package Brute;

import MRKNNGraph.Node;
import NNDescent.Neighbor;
import NNDescent.NeighborList;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author tibo
 */
class BruteReducer extends Reducer<Node, NeighborList, Node, NeighborList>{

    @Override
    protected void reduce(Node key, Iterable<NeighborList> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList new_neighbor_list = new NeighborList();
        
        for (NeighborList neighbors_list : values) {
            for (Neighbor n : neighbors_list) {
                new_neighbor_list.add(new Neighbor(n));
            }
        }
        
        context.write(key, new_neighbor_list);
    }
    
}
