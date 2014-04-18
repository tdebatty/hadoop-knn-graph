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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;

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
public class Bin extends Configured implements Tool {
    public static final String KEY_SIMILARITY = "Bin.Reducer.Similarity";
    public static final String KEY_PARSER = "Bin.Mapper.Parser";
    
    public StringParser string_parser;
    public SimilarityCalculator similarity_calculator;
    public String in;
    public String out;

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Bin(), args);
            System.exit(res);
            
        } catch (Exception ex) {
            Logger.getLogger(Bin.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
        
    @Override
    public int run(String[] args) throws Exception {
        in = args[0];
        out = args[1];
        
        string_parser = new DefaultStringParser();
        similarity_calculator = new DefaultSimilarityCalculator();
        
        // Configuration processed by ToolRunner
        return process(getConf());
        
    }
    
    public int process(Configuration conf) throws Exception {

        // Create a Job using the processed conf
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(Bin.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, in);
        
        job.getConfiguration().set(KEY_PARSER, toString(string_parser));
        job.getConfiguration().set(KEY_SIMILARITY, toString(similarity_calculator));
        
        job.setMapperClass(BinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        
        job.setReducerClass(BinReducer.class);
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
        Object o;
        try (ObjectInputStream ois = new ObjectInputStream( 
                new ByteArrayInputStream(data))) {
            o = ois.readObject();
        }
        return o;
   }

    /** Write the object to a Base64 string.
     * @param o
     * @return 
     * @throws java.io.IOException */
    public static String toString( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
        }
        return new String(Base64.encodeBase64(baos.toByteArray()));
    }
}

class BinMapper  extends Mapper<LongWritable, Text, Text, Node> {

    SpamSum ss;
    Node n;
    Text return_key;
    
    StringParser sp;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        ss = new SpamSum();
        n = new Node();
        return_key = new Text();
        
        try {
            sp = (StringParser) Bin.fromString(context.getConfiguration().get(Bin.KEY_PARSER));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BinMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        
        String s = value.toString();
        try {
            n = sp.parse(s);

            //ss = new SpamSum();
            ss.HashString(n.value);
            //System.out.println(ss.Left().substring(1, 2));
            
            return_key.set("0" + ss.Left().substring(0, 1));
            context.write(return_key, n);
            
            return_key.set("1" + ss.Left().substring(1, 2));
            context.write(return_key, n);
            
        } catch (IOException | InterruptedException ex) {
            System.err.println("Failed to parse " + s);
        }
    }
}

class BinReducer extends Reducer<Text, Node, NullWritable, Text> {
    SimilarityCalculator sc;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            sc = (SimilarityCalculator) Bin.fromString(context.getConfiguration().get(Bin.KEY_SIMILARITY));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BinReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    
    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Node> nodes = new ArrayList<>();
        
        for (Node n : values) {
            for (int i = 0; i < nodes.size(); i++) {
                Node other = nodes.get(i);
                double similarity = sc.similarity(n, other);

                Edge e = new Edge(n, other, similarity);
                context.write(NullWritable.get(), new Text(e.toString()));
            }
            
            // We need to create a copy of n, as Hadoop will reuse the same
            // object, and simply overwrite the values
            // see Node.readFields(...)
            nodes.add(new Node(n));
        }
    }
}