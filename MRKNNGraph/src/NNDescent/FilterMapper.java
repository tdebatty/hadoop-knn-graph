package NNDescent;

import MRKNNGraph.Node;
import static info.debatty.stringsimilarity.JaroWinkler.Similarity;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author tibo
 */
class FilterMapper extends Mapper<LongWritable, Text, Node, NeighborList> {
    
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
                nl.add(new Neighbor(other_node, Similarity(node.value, other_node.value)));
            }
            
            //System.out.println("Node " + node.id + " has " + nl.size() + " candidate neighbors");
            context.write(node, nl);
        }
        
    }
    
}
