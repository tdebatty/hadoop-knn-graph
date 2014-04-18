package NNCtph;

import MRKNNGraph.Node;
import info.debatty.stringsimilarity.JaroWinkler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Main extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Main(), args);
            System.exit(res);
            
        } catch (Exception ex) {
            Logger.getLogger(Bin.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();
        
        Bin bin = new Bin();
        bin.in = args[0];
        bin.out = args[1] + "-temp";
        
        bin.similarity_calculator = new MySimilarityCalculator();
        bin.string_parser = new MyStringParser();
        
        if (bin.process(conf) == 1) return 1;
        
        Prune prune = new Prune();
        prune.in = args[1] + "-temp";
        prune.out = args[1];
        prune.k = Integer.valueOf(args[2]);
        return prune.process(conf);
    }
    
    
    // Inner classes must be declared as static to be serializable!
    // Anonymous classes are not serializable either!
    static class MyStringParser implements StringParser {
        @Override
        public Node parse(String s) {
            String[] pieces = s.split("\"", 4);
            Node n = new Node();
            n.id = pieces[1];
            n.value = pieces[3];
            return n;
        }
    }
    
    static class MySimilarityCalculator implements SimilarityCalculator {
        @Override
        public double similarity(Node n1, Node n2) {
            return JaroWinkler.Similarity(n1.value, n2.value);
        }
    }
    
}
