package Brute;

import MRKNNGraph.Node;
import NNDescent.NeighborList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Brute extends Configured implements Tool
{
    public static void main (String[] args) {
        try {
            ToolRunner.run(new Brute(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(Brute.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Brute() {
        
    }
    
    public String in = "/spam100";
    public String out = "/brute-spam100";

    @Override
    public int run(String[] args) throws Exception {
        
        in = args[0];
        out = args[1];
        
        System.out.println("Brute Force KNN graph building");
        System.out.println("==============================");
        System.out.println("in: " + in);
        System.out.println("out: " + out);
        
        long start = System.currentTimeMillis();
        
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(CartesianTextInputFormat.class);
        CartesianTextInputFormat.setLeftInputhPath(job, in);
        CartesianTextInputFormat.setRightInputhPath(job, in);
        
        job.setMapperClass(BruteMapper.class);
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(NeighborList.class);
        
        job.setCombinerClass(BruteReducer.class);
        
        job.setReducerClass(BruteReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NeighborList.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));
        
        int return_value = job.waitForCompletion(true) ? 0 : 1;
        
        System.out.println("Running time: " + (System.currentTimeMillis() - start) / 1000 + " sec");
        
        return return_value;
    }
}
