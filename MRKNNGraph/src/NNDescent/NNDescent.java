package NNDescent;

import MRKNNGraph.Node;

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

public class NNDescent extends Configured implements Tool
{
    public static void main (String[] args) {
        try {
            ToolRunner.run(new NNDescent(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(NNDescent.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public NNDescent() {
    }
    
    public int iteration = 0;
    public int max_iterations = 2;
    public String in = "/spam100";
    public String out = "/dataout";

    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        max_iterations = Integer.valueOf(args[2]);
        
        System.out.println("NNDescent");
        System.out.println("=========");
        System.out.println("In: " + in);
        System.out.println("Out: " + out);
        System.out.println("Max iterations: " + max_iterations);
        
        long start = System.currentTimeMillis();
        
        
        Randomize();
        Merge();
        
        System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
        
        while (true) {
            iteration++;
            System.out.println("Iteration : " + iteration);
            System.out.println("---------------");
            
            if (!Reverse()) {
                return 1;
            }
            
            if (!Filter()) {
                return 1;
            }
            
            System.out.println("Running time: " + (System.currentTimeMillis() - start)/1000 + " sec");
            
            if (iteration == max_iterations) {
                break;
            }
        }
        return 0;
    }
    

    
    
    /**
     * 
     * @return true is job succeeds
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException 
     */
    protected boolean Filter() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Filter...");
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-" + iteration + "-reverse"));
        
        job.setMapperClass(FilterMapper.class);
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(NeighborList.class);

        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NeighborList.class);

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
    protected boolean Reverse() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Reverse...");
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-" + (iteration - 1) + "-filter"));
        
        job.setMapperClass(ReverseMapper.class);
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(Neighbor.class);

        // ReverseReducer is a simple merge of neighbors into a neighborlist
        job.setReducerClass(ReverseReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NeighborList.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-" + iteration + "-reverse"));
        
        return job.waitForCompletion(true);
    }

    protected int Merge() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Merge...");
        
        // Create a Job using the processed conf
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path(out + "-0-randomize"));
        
        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(Neighbor.class);

        // 
        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NeighborList.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-0-filter"));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    protected int Randomize() throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Randomize...");
        // Create a Job using the processed conf
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, in);
        
        job.setMapperClass(RandomizeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Node.class);

        job.setReducerClass(RandomizeReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(Neighbor.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out + "-0-randomize"));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}