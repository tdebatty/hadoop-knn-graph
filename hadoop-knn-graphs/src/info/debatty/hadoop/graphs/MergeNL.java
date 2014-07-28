package info.debatty.hadoop.graphs;

import info.debatty.graphs.Neighbor;
import info.debatty.graphs.NeighborList;
import info.debatty.graphs.Node;
import java.io.IOException;
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
public class MergeNL extends Configured implements Tool {
    public static void main (String[] args) {
        try {
            ToolRunner.run(new MergeNL(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(MergeNL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public MergeNL() {
        
    }
    
    public String in;
    public String out;
    
    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        
        return Run(getConf());
    }
    
    public int Run (Configuration conf) throws Exception {
        System.out.println("Merge Neighbor Lists");
        System.out.println("======================");
        System.out.println("in: " + in);
        System.out.println("out: " + out);
        
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(in));

        job.setMapperClass(MergeNLMapper.class);
        job.setMapOutputKeyClass(NodeWritable.class);
        job.setMapOutputValueClass(NeighborListWritable.class);
        
        job.setReducerClass(MergeNLReducer.class);
        job.setOutputKeyClass(NodeWritable.class);
        job.setOutputValueClass(NeighborListWritable.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class MergeNLMapper extends Mapper<LongWritable, Text, NodeWritable, NeighborListWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        try {
            // Input should look like
            // Node tab Neighbor
            String[] input = value.toString().split("\t", 2);
            Node n = Node.parseString(input[0]);
            NeighborList nl = NeighborList.parseString(input[1]);
            context.write(new NodeWritable(n), new NeighborListWritable(nl));
            
        } catch (Exception ex) {
            System.out.println("Could not parse : " + value.toString());
            context.getCounter("MergeNL", "Failed parsing").increment(1);
        }
    }
}

class MergeNLReducer extends Reducer<NodeWritable, NeighborListWritable, NodeWritable, NeighborListWritable> {

    @Override
    protected void reduce(NodeWritable key, Iterable<NeighborListWritable> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList neighbor_list = new NeighborList(10);
        for (NeighborListWritable nl : values) {
            for (Neighbor n : nl.get()) {

                neighbor_list.add(n);
            }
        }
        context.write(key, new NeighborListWritable(neighbor_list));
    }
}