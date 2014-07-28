package info.debatty.hadoop.graphs;

import info.debatty.graphs.NeighborList;
import info.debatty.graphs.Node;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class CompareNL  extends Configured implements Tool {
    public static void main (String[] args) {
        try {
            ToolRunner.run(new CompareNL(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(CompareNL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public CompareNL() {
        
    }
    
    public String in1;
    public String in2;
    
    @Override
    public int run(String[] args) throws Exception {
        in1 = args[0];
        in2 = args[1];
        
        return Run(getConf());
    }

    public int Run(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Compare Neighbor Lists");
        System.out.println("======================");
        System.out.println("in1: " + in1);
        System.out.println("in2: " + in2);
        
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(in1));
        TextInputFormat.addInputPath(job, new Path(in2));

        job.setMapperClass(CompareNLMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborListWritable.class);
        
        job.setReducerClass(CompareNLReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class CompareNLMapper extends Mapper<LongWritable, Text, NodeWritable, NeighborListWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        try {
            String[] input = value.toString().split("\t", 2);
            Node n = Node.parseString(input[0]);
            NeighborList neighbors_list = NeighborList.parseString(input[1]);

            context.write(new NodeWritable(n), new NeighborListWritable(neighbors_list));
            
        } catch (Exception ex) {
            Logger.getLogger(this.getClass().getName()).warning("Failed to parse " + value.toString());
            context.getCounter("Compare", "Failed parsing").increment(1);
        }
    }
}

class CompareNLReducer extends Reducer<NodeWritable, NeighborListWritable, NullWritable, NullWritable> {

    @Override
    protected void reduce(NodeWritable key, Iterable<NeighborListWritable> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList nl1;
        NeighborList nl2;
        
        // Each reduce key should receive 2 NeigborLists
        try {
            nl1 = values.iterator().next().get();
            nl2 = values.iterator().next().get();
            
        } catch (Exception ex) {
            context.getCounter("CompareNLReducer", "# Node has less than 2 NeighborLists").increment(1);
            return;
        }
        
        int count = nl1.CountCommonValues(nl2);
        context.getCounter("CompareNLReducer", "Count Commons").increment(count);
    
    }
    
    
}