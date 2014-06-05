package MRKNNGraph;

import NNDescent.Neighbor;
import NNDescent.NeighborList;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Edge2NL  extends Configured implements Tool
{
    public static void main (String[] args) {
        try {
            ToolRunner.run(new Edge2NL(), args);
            
        } catch (Exception ex) {
            Logger.getLogger(Edge2NL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public Edge2NL() {
        
    }
    
    public String in;
    public String out;
    
    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        
        System.out.println("Convert Edges to Neighbor Lists");
        System.out.println("===============================");
        System.out.println("in: " + in);
        System.out.println("out: " + out);
        
        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(in));

        job.setMapperClass(Edge2NLMapper.class);
        job.setMapOutputKeyClass(Node.class);
        job.setMapOutputValueClass(Neighbor.class);
        
        job.setReducerClass(Edge2NLReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NeighborList.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(out));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class Edge2NLMapper extends Mapper<LongWritable, Text, Node, Neighbor>{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        Edge edge = Edge.parseString(value.toString());
        Neighbor neighbor = new Neighbor(edge.n2, edge.similarity);
        
        context.write(edge.n1, neighbor);
        
    }
}

class Edge2NLReducer extends Reducer<Node, Neighbor, Node, NeighborList> {

    @Override
    protected void reduce(Node key, Iterable<Neighbor> values, Context context)
            throws IOException, InterruptedException {
        
        NeighborList nl = new NeighborList();
        
         for (Neighbor n : values) {
             nl.add(new Neighbor(n));
         }
         
         for (Neighbor n : nl) {
             context.getCounter("Edge2NL", "Total similarity (/1000)").increment((long) (n.similarity * 1000));
         }
         
         context.write(key, nl);
    
    }
    
    
}