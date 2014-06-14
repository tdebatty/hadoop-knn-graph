package NNCtph;

import MRKNNGraph.Edge;
import MRKNNGraph.Node;

import info.debatty.stringsimilarity.JaroWinkler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class NNCtph extends Configured implements Tool {
    
    public static final String KEY_SIMILARITY = "NNCTPH.Similarity";
    public static final String KEY_PARSER = "NNCTPH.Parser";
    public static final String KEY_K = "NNCTPH.k";
    public static final String KEY_STAGES = "NNCTPH.Stages";
    public static final String KEY_LENGTH = "NNCTPH.key-length";
    public static final String KEY_CHARACTERS = "NNCTPH.key-characters";

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
    public int stages = 2;
    public int key_length = 1; // characters (1 => 64 bins, 2 => 4096 bins)
    public int key_characters = 10;
    
    
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
        if (args.length != 6) {
            System.out.println("Usage: NNCtph <in> <out> <k> <stages> <key_length> <key_characters>");
            return 1;
        }
        
        in = args[0];
        out = args[1];
        k = Integer.valueOf(args[2]);
        stages = Integer.valueOf(args[3]);
        key_length = Integer.valueOf(args[4]);
        key_characters = Integer.valueOf(args[5]);
        
        
        string_parser = new MyStringParser();
        similarity_calculator = new MySimilarityCalculator();
        
        long start = System.currentTimeMillis();
        
        System.out.println("NNCtph");
        System.out.println("======");
        System.out.println("In:     " + in);
        System.out.println("Out:    " + out);
        System.out.println("k:      " + k);
        System.out.println("Stages: " + stages);
        System.out.println("Key length: " + key_length);
        System.out.println("Possible characters in key: " + key_characters);
        System.out.println("=> # bins: " + (int) Math.pow(key_characters, key_length));
        System.out.println("String parser: " + string_parser.getClass().getName());
        System.out.println("Similarity metric: " + similarity_calculator.getClass().getName());
        
        int return_value =  run();
        
        System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
        
        return return_value;
    }
    
    public int run() throws Exception {

        // Create a Job using the processed conf
        Job job = new Job(getConf(), this.getClass().getName());
        job.setJarByClass(NNCtph.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        
        job.getConfiguration().set(KEY_PARSER, toString(string_parser));
        job.getConfiguration().set(KEY_SIMILARITY, toString(similarity_calculator));
        job.getConfiguration().setInt(KEY_K, k);
        job.getConfiguration().setInt(KEY_STAGES, stages);
        job.getConfiguration().setInt(KEY_LENGTH, key_length);
        job.getConfiguration().setInt(KEY_CHARACTERS, key_characters);
        
        // !!!!!! Give 4GB per task !!!!!!! //
        job.getConfiguration().set("mapred.child.java.opts", "-Xmx4096m");
        
        job.setMapperClass(NNCTPHMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        
        job.setReducerClass(NNCTPHReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    /** Read the object from Base64 string.
     * @param s
     * @return 
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException */
   public static Object fromString( String s )
           throws IOException, ClassNotFoundException {
        byte [] data = Base64.decodeBase64(s );
        Object o = null;
        
        ObjectInputStream ois = new ObjectInputStream( 
                new ByteArrayInputStream(data));
        o = ois.readObject();
        
        return o;
   }

    /** Write the object to a Base64 string.
     * @param o
     * @return 
     * @throws java.io.IOException */
    public static String toString( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        
        return new String(Base64.encodeBase64(baos.toByteArray()));
    }
    
    
    // Inner classes must be static to be serializable!
    // Anonymous classes are not serializable either!
    static class MyStringParser implements StringParser {
        @Override
        public Node parse(String s) {
            String[] pieces = s.split("\"", 4);
            Node n = new Node();
            n.id.set(pieces[1]);
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


class NNCTPHMapper extends Mapper<LongWritable, Text, Text, Node> {

    SpamSum ss;
    Node n;
    Text return_key;
    
    StringParser sp;
    int stages = 0;
    private int key_length = 0;
    private int key_characters = 0;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        
        n = new Node();
        return_key = new Text();
        
        try {
            sp = (StringParser) NNCtph.fromString(context.getConfiguration().get(NNCtph.KEY_PARSER));
            stages = context.getConfiguration().getInt(NNCtph.KEY_STAGES, stages);
            key_length = context.getConfiguration().getInt(NNCtph.KEY_LENGTH, key_length);
            key_characters = context.getConfiguration().getInt(NNCtph.KEY_CHARACTERS, key_characters);
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NNCTPHMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        ss = new SpamSum(stages * key_length, key_characters);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        String s = value.toString();
        try {
            n = sp.parse(s);

            ss.HashString(n.value);
            //System.out.println(ss.Left().substring(1, 2));
            
            for (int i = 0; i < stages; i++) {
                return_key.set(i + "_" + ss.Left().substring(i*key_length, (i+1)*key_length));
                context.write(return_key, n);
            }
            
            
        } catch (Exception ex) {
            System.err.println("Failed to parse " + s);
            context.getCounter("NNCTPH", "# failed parsing").increment(1);
        }
    }
}

class NNCTPHReducer extends Reducer<Text, Node, NullWritable, Text> {
    SimilarityCalculator sc;
    int k = -1;
    int stages = -1;
    
    public static final int mb = 1024*1024;
    
    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        try {
            sc = (SimilarityCalculator) NNCtph.fromString(context.getConfiguration().get(NNCtph.KEY_SIMILARITY));
            k = context.getConfiguration().getInt(NNCtph.KEY_K, k);
            stages = context.getConfiguration().getInt(NNCtph.KEY_STAGES, stages);
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NNCTPHReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context)
            throws IOException, InterruptedException {
        
        // Compute "this_k"
        // For e.g. if k = 10 and stages = 2
        // this reducer only has to find 5 edges
        String[] key_pieces = key.toString().split("_");
        int stage = Integer.valueOf(key_pieces[0]);
        int this_k = k / stages;
        if (stage == (stages - 1)) {
            this_k += k % stages;
        }
        
        //this_k = k;
        
        // First compute all pairwize similarities
        // add similarities and nodes to an arraylist
        // total number is not known for now...
        
        ArrayList<Node> nodes = new ArrayList<Node>();
        for (Node n : values) {
            // We need to create a copy of n, as Hadoop will reuse the same
            // object, and simply overwrite the values
            // see Node.readFields(...)
            nodes.add(new Node(n));
        }
        
        int n = nodes.size();
        System.out.println(n);
        
        //Runtime runtime = Runtime.getRuntime();
        //System.out.println("Fetched " + n + " nodes");
        //System.out.println("Used Memory:"
        //    + (runtime.totalMemory() - runtime.freeMemory()) / mb  + "MB");
        

        int pos = 0;
        double[] similarities = new double[n * (n-1) / 2];
        for (int j = 0; j < n; j++) {
            context.progress();
            
            Node node = nodes.get(j);
            for (int i = 0; i < j; i++) {
                double sim = sc.similarity(node, nodes.get(i));
                similarities[pos] = sim;
                //System.out.println(node.value + " <=> " + nodes.get(i).value + " : " + sim);
                pos++;
            }            
        }
        context.getCounter("NNCTPH", "computed similarities").increment(pos);
        
        
        //System.out.println("Computed " + pos + " similarities");
        //System.out.println("Used Memory:"
        //    + (runtime.totalMemory() - runtime.freeMemory()) / mb  + "MB");
        
        
        /**
         *   
         * i\j|| 0 | 1 | 2 | 3 | 4 
         * =========================
         * 0  || x |   |   |   |
         * 1  || 0 | x |   |   |
         * 2  || 1 | 2 | x |   |
         * 3  || 3 | 4 | 5 | x |
         * 4  || 6 | 7 | 8 | 9 | x
         * 
         * for node 2 (i = 2),
         * similarities are located at 1, 2, 5, 8
         * corresponding to nodes   j= 0, 1, 2, 3
         */
        
        
        double similarity;
        // For each node, find the k nearest neighbors
        for (int i = 0; i < n; i++) {
            context.progress();
            
            Node node = nodes.get(i);
            PriorityQueue<Edge> edges = new PriorityQueue<Edge>(this_k);
            for (int j = 0; j < n; j++) {
                
                
                if (i == j) {
                    continue;
                    
                }
                
                if (j < i) {
                    similarity = similarities[i * (i-1)/2 + j];
                    
                } else {
                    similarity = similarities[j * (j-1) / 2 + i];
                    
                }
                
                //System.out.println(node.value + " <=> " + nodes.get(j).value + " : " + similarity);
            
                if (edges.size() < this_k) {
                    edges.add(new Edge(
                            node,
                            nodes.get(j),
                            similarity));
                    continue;

                }

                if (similarity > edges.peek().similarity) {
                    edges.poll();
                    edges.add(new Edge(
                            node,
                            nodes.get(j),
                            similarity));
                }
            }

            for (Edge e : edges) {
                context.getCounter("NNCTPH", "Total similarity (x1000)").increment(
                        (long) (1000 * e.similarity));
                context.write(NullWritable.get(), new Text(e.toString()));
            }
        }
    }
}