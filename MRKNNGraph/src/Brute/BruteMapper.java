package Brute;

import MRKNNGraph.Node;
import NNCtph.DefaultStringParser;
import NNDescent.Neighbor;
import NNDescent.NeighborList;
import static info.debatty.stringsimilarity.JaroWinkler.Similarity;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author tibo
 */
class BruteMapper  extends Mapper<Text, Text, Node, NeighborList> {
    private DefaultStringParser sp;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        sp = new DefaultStringParser();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println(key.toString());
        Node node1 = sp.parse(key.toString());
        Node node2 = sp.parse(value.toString());
        
        NeighborList neighbors_list = new NeighborList();
        context.getCounter("Brute force KNN graph builder", "Similarities").increment(1);
        double similarity = Similarity(node1.value, node2.value);
        neighbors_list.add(new Neighbor(node2, similarity));
        context.write(node1, neighbors_list);
    }
    
    
    
    
}
