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
        Node node1, node2;
        try {
            node1 = sp.parse(key.toString());
            node2 = sp.parse(value.toString());
        } catch (Exception ex) {
            return;
        }
        
        if (node1.equals(node2)) {
            return;
        }

        NeighborList neighbors_list = new NeighborList();
        context.getCounter("BRUTE", "Similarities").increment(1);
        double similarity = Similarity(node1.value, node2.value);
        neighbors_list.add(new Neighbor(node2, similarity));
        context.write(node1, neighbors_list);
        
    }
}
