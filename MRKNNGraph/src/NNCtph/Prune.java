package NNCtph;

import MRKNNGraph.Edge;
import MRKNNGraph.Node;

import java.io.IOException;
import java.util.PriorityQueue;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Prune extends Configured implements Tool {
    
    public static final String KEY_K = "NNCtph.Prune.k";
    
    public String in = "";
    public String out = "";
    public int k = 10;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new Prune(), args));
            
        } catch (Exception ex) {
            Logger.getLogger(Prune.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        if (args.length == 3) {
            k = Integer.valueOf(args[2]);
        }
        
        return process();
    }
    
    public int process() throws Exception  {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a Job using the processed conf
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        
        job.getConfiguration().setInt(
                KEY_K,
                k);
        
        job.setMapperClass(PruneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Edge.class);
        
        job.setReducerClass(PruneReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }    
}

class PruneMapper extends Mapper<LongWritable, Text, Text, Edge> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        String s = value.toString();
        String[] pieces = s.split(";");
        
        Edge e;
        e = new Edge(
                new Node(pieces[0]),
                new Node(pieces[1]),
                Double.valueOf(pieces[2])
        );
        
        context.write(new Text(e.n1.id), e);
        
        e = new Edge(
                new Node(pieces[1]),
                new Node(pieces[0]),
                Double.valueOf(pieces[2])
        );
        context.write(new Text(e.n1.id), e);   
    }
}

class PruneReducer extends Reducer<Text, Edge, NullWritable, Text> {
    int k = 10;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        this.k = context.getConfiguration().getInt(Prune.KEY_K, k);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Edge> values, Context context)
            throws IOException, InterruptedException {
        
        
        PriorityQueue<Edge> edges = new PriorityQueue<>(k);
        
        for(Edge e : values) {
            if (edges.size() < k &&
                !edges.contains(e)) {
                edges.add(new Edge(e));
                continue;
                
            }
            
            if (e.similarity > edges.peek().similarity &&
                !edges.contains(e)) {
                edges.poll();
                edges.add(new Edge(e));
            }
        }
        
        for (Edge e : edges) {
            context.write(NullWritable.get(), new Text(e.toString()));
        }
    
    }
    
}