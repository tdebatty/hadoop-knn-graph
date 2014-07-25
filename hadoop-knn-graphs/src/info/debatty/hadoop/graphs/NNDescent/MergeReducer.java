package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.NeighborList;
import info.debatty.hadoop.graphs.NeighborListWritable;
import info.debatty.hadoop.graphs.NeighborWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author tibo
 */
class MergeReducer extends Reducer<NodeWritable, NeighborWritable, NodeWritable, NeighborListWritable> {

    @Override
    protected void reduce(NodeWritable key, Iterable<NeighborWritable> values, Context context)
            throws IOException, InterruptedException {
        
        
        NeighborList nl = new NeighborList();
        for (NeighborWritable n : values) {
            nl.add(n.get());
        }
        
        context.write(key, new NeighborListWritable(nl));
    }
    
}
