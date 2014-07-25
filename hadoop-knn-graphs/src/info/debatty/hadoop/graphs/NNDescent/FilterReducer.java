package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.hadoop.graphs.NeighborListWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

class FilterReducer extends Reducer<NodeWritable, NeighborListWritable, NodeWritable, NeighborListWritable> {
    public int k = 10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        k = context.getConfiguration().getInt(NNDescent.KEY_K, k);
    }

    @Override
    protected void reduce(NodeWritable key, Iterable<NeighborListWritable> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList new_neighborlist = new NeighborList(k);
        
        for(NeighborListWritable nl : values) {
            for (Neighbor neighbor : nl.get()) {
                new_neighborlist.add(neighbor);
                
            }
        }
        
        context.write(key, new NeighborListWritable(new_neighborlist));   
    }
}

