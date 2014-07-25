package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.SimilarityInterface;
import info.debatty.hadoop.graphs.*;
import info.debatty.hadoop.util.ObjectSerialize;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NNDescent extends Configured implements Tool {
    
    public static final String KEY_K = "NNDescent.k";
    public static final String KEY_SIMILARITY = "NNDescent.similarity";
    public static final String KEY_PARSER = "NNDescent.parser";
    
    public static void main (String[] args) {
        try {
            ToolRunner.run(new NNDescent(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(NNDescent.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public int k = 10;
    public int iterations = 2;
    public String in = "/spam100";
    public String out = "/dataout";
    public SimilarityInterface similarity;
    public StringParserInterface string_parser;
    
    protected int iteration = 0;
    protected Configuration conf;
    
    public long running_time = 0;
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: " + this.getClass().getCanonicalName() + " <in> <out> <k> <max_iterations>");
            return 1;
        }
        
        in = args[0];
        out = args[1];
        k = Integer.valueOf(args[2]);
        iterations = Integer.valueOf(args[3]);
        similarity = new DefaultSimilarity();
        string_parser = new DefaultStringParser();
        
        Run(getConf());
        Print();
        
        return 0;
    }
    
    public boolean Run(Configuration conf) throws Exception {
        this.conf = conf;
        long start = System.currentTimeMillis();
        
        Init();
        
        System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
        
        while (true) {
            iteration++;
            
            System.out.println("Iteration : " + iteration);
            System.out.println("---------------");
            
            if (!Reverse()) {
                return false;
            }
            
            if (!Filter()) {
                return false;
            }
            
            System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
            
            if (iteration == iterations) {
                break;
            }
        }
        
        running_time = (System.currentTimeMillis() - start) / 1000;
        return true;
    }
    
    public void Print() {
        System.out.println(this.getClass().getName());
        System.out.println("=========");
        System.out.println("In:   " + in);
        System.out.println("Out:  " + out);
        System.out.println("Iterations: " + iterations);
        System.out.println("K:    " + k);
        System.out.println("Similarity: " + similarity.getClass().getName());
        System.out.println("String parser: " + string_parser.getClass().getName());
        System.out.println("Running time: " + running_time + "sec");
    }

    
    /**
     * 
     * @return true is job succeeds
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException 
     */
    public boolean Filter() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Filter...");
        
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.getConfiguration().setInt(KEY_K, k);
        job.getConfiguration().set(KEY_SIMILARITY, ObjectSerialize.serialize(similarity));
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-" + iteration + "-reverse"));
        
        job.setMapperClass(FilterMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborListWritable.class);

        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-" + iteration + "-filter"));
        
        return job.waitForCompletion(true);
    }
    
    /**
     * 
     * @return true if job succeeds
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException 
     */
    public boolean Reverse() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Reverse...");
        
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-" + (iteration - 1) + "-filter"));
        
        job.setMapperClass(ReverseMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborWritable.class);

        // ReverseReducer is a simple merge of neighbors into a neighborlist
        job.setReducerClass(ReverseReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-" + iteration + "-reverse"));
        
        return job.waitForCompletion(true);
    }

    public int Merge() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Merge...");
        
        // Create a Job using the processed conf
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-0-randomize"));
        
        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborWritable.class);

        // 
        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-0-filter"));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int Randomize() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Randomize...");
        
        // Create a Job using the processed conf
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.getConfiguration().set(KEY_PARSER, ObjectSerialize.serialize(string_parser));
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, in);
        
        job.setMapperClass(RandomizeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NodeWritable.class);

        job.setReducerClass(RandomizeReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-0-randomize"));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void Init() throws Exception {
        Randomize();
        Merge();
    }
}