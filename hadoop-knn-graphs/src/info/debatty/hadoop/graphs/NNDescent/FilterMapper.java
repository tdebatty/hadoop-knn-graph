package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.graphs.Node;
import info.debatty.graphs.SimilarityInterface;
import info.debatty.hadoop.graphs.NeighborListWritable;
import info.debatty.hadoop.graphs.NodeWritable;
import info.debatty.hadoop.util.ObjectSerialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class FilterMapper extends Mapper<LongWritable, Text, NodeWritable, NeighborListWritable> {
    
    SimilarityInterface similarity;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
        try {
            similarity = (SimilarityInterface) ObjectSerialize.unserialize(
                    context.getConfiguration().get(NNDescent.KEY_SIMILARITY));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(FilterMapper.class.getName()).log(Level.SEVERE, null, ex);
            System.err.println("Could not load similarity metric");
            System.exit(1);
        }
    }
    
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // Input should look like
        // Node tab NeighborList
        String[] input = value.toString().split("\t", 2);
        Node n = Node.parseString(input[0]);
        NeighborList neighbors_list = NeighborList.parseString(input[1]);
        
        ArrayList<Node> nodes_list = new ArrayList<Node>();
        nodes_list.add(n);
        for (Neighbor neighbor : neighbors_list) {
            nodes_list.add(neighbor.node);
            
        }
        
        for (Node node : nodes_list) {
            NeighborList nl = new NeighborList();
            
            for (Node other_node : nodes_list) {
                if (node.equals(other_node)) {
                    continue;
                }
                context.getCounter("NNDescent", "Similarities").increment(1);
                nl.add(new Neighbor(other_node, similarity.similarity(node, other_node)));
            }
            
            context.write(
                    new NodeWritable(node),
                    new NeighborListWritable(nl));
        }
        
    }
}