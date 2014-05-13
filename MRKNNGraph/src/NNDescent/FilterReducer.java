/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package NNDescent;

import MRKNNGraph.Node;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author tibo
 */
class FilterReducer extends Reducer<Node, NeighborList, Node, NeighborList> {
    public int k = 10;

    @Override
    protected void reduce(Node key, Iterable<NeighborList> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList new_neighborlist = new NeighborList();
        
        for(NeighborList nl : values) {
            for (Neighbor neighbor : nl) {
                new_neighborlist.add(new Neighbor(neighbor));
                
            }
        }
        
        context.write(key, new_neighborlist);
        
    }
    
    
    
}
