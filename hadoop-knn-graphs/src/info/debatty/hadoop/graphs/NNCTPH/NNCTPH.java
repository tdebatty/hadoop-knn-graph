package info.debatty.hadoop.graphs.NNCTPH;

import info.debatty.graphs.*;
import info.debatty.hadoop.graphs.*;
import info.debatty.hadoop.util.ObjectSerialize;
import info.debatty.spamsum.SpamSum;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class NNCTPH extends Configured implements Tool {
    
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
            int res = ToolRunner.run(new NNCTPH(), args);
            System.exit(res);
            
        } catch (Exception ex) {
            Logger.getLogger(NNCTPH.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String in = "";
    public String out = "";
    public int k = 10;
    public int stages = 2;
    public int key_length = 1; // characters (1 => 64 bins, 2 => 4096 bins)
    public int key_characters = 10;
    public SimilarityInterface similarity;
    public StringParserInterface string_parser;
    
    public long running_time = 0;


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
            System.out.println("Usage: " + this.getClass().getCanonicalName() + " <in> <out> <k> <stages> <key_length> <key_characters>");
            return 1;
        }
        
        in = args[0];
        out = args[1];
        k = Integer.valueOf(args[2]);
        stages = Integer.valueOf(args[3]);
        key_length = Integer.valueOf(args[4]);
        key_characters = Integer.valueOf(args[5]);
        
        string_parser = new DefaultStringParser();
        similarity = new DefaultSimilarity();
        
        Run(getConf());
        Print();
        
        return 0;
    }
    
    public boolean Run(Configuration jobconf) throws Exception {
        long start = System.currentTimeMillis();

        // Create a Job using the processed conf
        Job job = new Job(jobconf, this.getClass().getName());
        job.setJarByClass(NNCTPH.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        
        job.getConfiguration().set(KEY_PARSER, ObjectSerialize.serialize(string_parser));
        job.getConfiguration().set(KEY_SIMILARITY, ObjectSerialize.serialize(similarity));
        job.getConfiguration().setInt(KEY_K, k);
        job.getConfiguration().setInt(KEY_STAGES, stages);
        job.getConfiguration().setInt(KEY_LENGTH, key_length);
        job.getConfiguration().setInt(KEY_CHARACTERS, key_characters);
        
        // !!!!!! Give 4GB per task !!!!!!! //
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx4096m");
        
        // !!!!!! Give 2GB per task !!!!!!! //
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        
        job.setMapperClass(NNCTPHMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NodeWritable.class);
        
        job.setReducerClass(NNCTPHReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));
        
        boolean return_value = job.waitForCompletion(true);
        
        running_time = (System.currentTimeMillis() - start) / 1000;
        
        return return_value;
    }
    
    public void Print() {
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
        System.out.println("Similarity metric: " + similarity.getClass().getName());
        System.out.println("Running time: " + running_time);

    }
}


class NNCTPHMapper extends Mapper<LongWritable, Text, Text, NodeWritable> {

    protected SpamSum ss;
    protected StringParserInterface sp;
    protected int stages = 0;
    protected int key_length = 0;
    protected int key_characters = 0;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        
        try {
            sp = (StringParserInterface) ObjectSerialize.unserialize(context.getConfiguration().get(NNCTPH.KEY_PARSER));
            stages = context.getConfiguration().getInt(NNCTPH.KEY_STAGES, stages);
            key_length = context.getConfiguration().getInt(NNCTPH.KEY_LENGTH, key_length);
            key_characters = context.getConfiguration().getInt(NNCTPH.KEY_CHARACTERS, key_characters);
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NNCTPHMapper.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        
        ss = new SpamSum(key_length + stages - 1, key_characters);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        Node n;
        try {
            n = sp.parse(value.toString());
            
        } catch (Exception ex) {
            System.err.println("Failed to parse " + value.toString());
            context.getCounter("NNCTPH", "# failed parsing").increment(1);
            return;
        }

        try {
            ss.HashString(n.value.toString());
        } catch (Exception ex) {
            System.err.println("Failed to hash " + value.toString());
            context.getCounter("NNCTPH", "# failed hashing").increment(1);
            return;
        }
        
        String hash = ss.Left();

        for (int stage = 0; stage < stages; stage++) {
            if (hash.length() < stage + key_length) {
                // In some case it is not possible to create a hash this long
                return;
            }

            context.write(
                    new Text(stage + "_" + hash.substring(stage, stage + key_length)),
                    new NodeWritable(n));
        }
    }
}

class NNCTPHReducer extends Reducer<Text, NodeWritable, NodeWritable, NeighborListWritable> {
    SimilarityInterface similarity;
    int k = -1;
    int stages = -1;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            similarity = (SimilarityInterface) ObjectSerialize.unserialize(context.getConfiguration().get(NNCTPH.KEY_SIMILARITY));
            k = context.getConfiguration().getInt(NNCTPH.KEY_K, k);
            stages = context.getConfiguration().getInt(NNCTPH.KEY_STAGES, stages);
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NNCTPHReducer.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
    
    @Override
    protected void reduce(Text key, Iterable<NodeWritable> values, Context context)
            throws IOException, InterruptedException {
        
        
        ArrayList<Node> nodes = new ArrayList<Node>();
        for (NodeWritable n : values) {
            // We need to create a copy of n, as Hadoop will reuse the same
            // object, and simply overwrite the values
            // see Node.readFields(...)
            nodes.add(n.get());
        }
        
        int n = nodes.size();
        HashMap<Node, NeighborList> neighborlists;

        
        if (n >= 500) {
            // -------------------------- NNDescent   -----------------------------
            NNDescent nnd = new NNDescent();
            nnd.nodes = nodes;
            nnd.K = k;
            nnd.delta = 0.01;
            nnd.rho = 0.5;
            nnd.similarity = similarity;
            nnd.callback = new ReducerNNDescentCallback(context);
            nnd.Run();

            context.getCounter("NNCTPH", "computed similarities").increment(nnd.computed_similarities);
            neighborlists = nnd.neighborlists;
            
            
        } else {
            // -------------------------- Brute-force -----------------------------
            Brute brute = new Brute();
            brute.K = k;
            brute.nodes = nodes;
            brute.similarity = similarity;
            brute.callback = new ReducerBruteCallback(context);
            brute.Run();
            
            context.getCounter("NNCTPH", "computed similarities").increment(n * (n-1) / 2);
            neighborlists = brute.neighborlists;
        }
        
        
        for (Node node : nodes) {
            context.write(
                    new NodeWritable(node),
                    new NeighborListWritable(neighborlists.get(node)));
        }
        
      
    }

    private static class ReducerNNDescentCallback implements NNDescentCallbackInterface {
        Context context;

        public ReducerNNDescentCallback(Context context) {
            this.context = context;
        }
        
        @Override
        public void call(int iteration, int computed_similarities, int c) {
            context.progress();
        }
    }

    private static class ReducerBruteCallback implements BruteCallbackInterface {

        Context context;
        
        public ReducerBruteCallback(Context context) {
            this.context = context;
        }

        @Override
        public void call(int node_id, int computed_similarities) {
            context.progress();
        }
    }
}