package info.debatty.hadoop.graphs.Brute;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.graphs.Node;
import info.debatty.hadoop.CartesianProduct.CartesianTextInputFormat;
import info.debatty.graphs.SimilarityInterface;
import info.debatty.hadoop.graphs.DefaultSimilarity;
import info.debatty.hadoop.graphs.DefaultStringParser;
import info.debatty.hadoop.graphs.NodeWritable;
import info.debatty.hadoop.graphs.NeighborListWritable;
import info.debatty.hadoop.graphs.StringParserInterface;
import info.debatty.hadoop.util.ObjectSerialize;
import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Brute extends Configured implements Tool {
    
    public static final String KEY_K = "Brute.k";
    public static final String KEY_SIMILARITY = "Brute.similarity";
    public static final String KEY_PARSER = "Brute.parser";
    
    
    public static void main (String[] args) {
        try {
            ToolRunner.run(new Brute(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(Brute.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String in = "/spam100";
    public String out = "/brute-spam100";
    public int k = 10;
    public StringParserInterface string_parser;
    public SimilarityInterface similarity;
    
    public long running_time = 0;

    @Override
    public int run(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.out.println("Usage: " + this.getClass().getCanonicalName() + " <in> <out> <k>");
            return 1;
        }
        
        in = args[0];
        out = args[1];
        k = Integer.valueOf(args[2]);
        
        string_parser = new DefaultStringParser();
        similarity = new DefaultSimilarity();
        
        Run(getConf());
        Print();
        
        return 0;
    }
    
    public boolean Run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        
        Job job = Job.getInstance(conf);//  new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.getConfiguration().setInt(KEY_K, k);
        job.getConfiguration().set(KEY_PARSER, ObjectSerialize.serialize(string_parser));
        job.getConfiguration().set(KEY_SIMILARITY, ObjectSerialize.serialize(similarity));
        
        // Reduce split size to 8MB to get more mappers!
        job.getConfiguration().setInt("mapred.max.split.size", 8388608);
        job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize", 8388608);
        
        job.setInputFormatClass(CartesianTextInputFormat.class);
        CartesianTextInputFormat.setLeftInputhPath(job, in);
        CartesianTextInputFormat.setRightInputhPath(job, in);
        
        job.setMapperClass(BruteMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborListWritable.class);
        
        job.setCombinerClass(BruteReducer.class);
        
        job.setReducerClass(BruteReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));
        
        boolean return_value = job.waitForCompletion(true);
        running_time = (System.currentTimeMillis() - start) / 1000;
        
        return return_value;
    }
    
    public void Print() {
        System.out.println("Brute Force KNN graph building");
        System.out.println("==============================");
        System.out.println("in: " + in);
        System.out.println("out: " + out);
        System.out.println("Running time: " + running_time + " sec");
    }
}

class BruteMapper  extends Mapper<Text, Text, NodeWritable, NeighborListWritable> {
    protected StringParserInterface sp;
    protected SimilarityInterface similarity;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        
        try {
            sp = (StringParserInterface) ObjectSerialize.unserialize(
                context.getConfiguration().get(Brute.KEY_PARSER));
            
            System.out.println(context.getConfiguration().get(Brute.KEY_PARSER));
        
            similarity = (SimilarityInterface) ObjectSerialize.unserialize(
                context.getConfiguration().get(Brute.KEY_SIMILARITY));
        
        } catch (Exception ex) {
            System.out.println("Could not load string parser and/or similarity metric");
            System.exit(1);
        }
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Node node1, node2;
        try {
            node1 = sp.parse(key.toString());
            node2 = sp.parse(value.toString());
            
        } catch (Exception ex) {
            context.getCounter("BRUTE", "Failed parsing").increment(1);
            return;
        }
        
        if (node1.equals(node2)) {
            return;
        }

        context.getCounter("BRUTE", "Similarities").increment(1);
        
        double s = similarity.similarity(node1, node2);
        NeighborList neighbors_list = new NeighborList();
        neighbors_list.add(new Neighbor(node2, s));
        
        context.write(new NodeWritable(node1), new NeighborListWritable(neighbors_list));
    }
}


class BruteReducer extends Reducer<NodeWritable, NeighborListWritable, NodeWritable, NeighborListWritable>{

    public int k = 10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        k = context.getConfiguration().getInt(Brute.KEY_K, k);
    }
    
    
    
    @Override
    protected void reduce(NodeWritable key, Iterable<NeighborListWritable> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList new_neighbor_list = new NeighborList(k);
        
        for (NeighborListWritable writable_neighbor_list : values) {
            for (Neighbor n : writable_neighbor_list.get()) {
                new_neighbor_list.add(n);
            }
        }
        
        context.write(key, new NeighborListWritable(new_neighbor_list));
    }
}