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
public class NNCtph extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new NNCtph(), args);
            System.exit(res);
            
        } catch (Exception ex) {
            Logger.getLogger(NNCtph.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String in = "";
    public String out = "";
    public int k = 10;
    
    public SimilarityCalculator similarity_calculator;
    public StringParser string_parser;
    
    public NNCtph() {
        string_parser = new DefaultStringParser();
        similarity_calculator = new DefaultSimilarityCalculator();
    }

    /**
     * If running from the command line...
     * 
     * @param args
     * @return
     * @throws Exception 
     */
    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        k = Integer.valueOf(args[2]);
        
        string_parser = new MyStringParser();
        similarity_calculator = new MySimilarityCalculator();
        
        long start = System.currentTimeMillis();
        
        System.out.println("NNCtph");
        System.out.println("======");
        System.out.println("In: " + in);
        System.out.println("Out: " + out);
        System.out.println("K: " + k);
        
        int return_value =  run();
        
        System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
        
        return return_value;
    }
    
    public int run() throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();
        
        Bin bin = new Bin();
        bin.in = in;
        bin.out = out + "-temp";
        
        bin.similarity_calculator = similarity_calculator;
        bin.string_parser = string_parser;
        
        if (bin.process(conf) == 1) return 1;
        
        Prune prune = new Prune();
        prune.in = out + "-temp";
        prune.out = out;
        prune.k = k;
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
