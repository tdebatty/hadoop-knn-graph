package NNCtph;

import MRKNNGraph.Edge;
import MRKNNGraph.Node;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
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
public class Bin extends Configured implements Tool {

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
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a Job using the processed conf
        Job job = new Job(conf, this.getClass().getName());
        job.setJarByClass(Bin.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, args[0]);
        
        Parser p = new Parser();
        job.getConfiguration().set("Bin.Mapper.Parser", toString(p));
        
        job.setMapperClass(BinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        
        job.setReducerClass(BinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    
    /** Read the object from Base64 string.
     * @param s
     * @return 
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException */
   public static Object fromString( String s )
           throws IOException, ClassNotFoundException {
        byte [] data = Base64Coder.decode( s );
        Object o;
        try (ObjectInputStream ois = new ObjectInputStream( 
                new ByteArrayInputStream(  data ) )) {
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
        try (ObjectOutputStream oos = new ObjectOutputStream( baos )) {
            oos.writeObject( o );
        }
        return new String( Base64Coder.encode( baos.toByteArray() ) );
    }


}

class Parser implements StringParser {

    @Override
    public Node parse(String s) {
        String[] pieces = s.split("\"", 4);
        Node n = new Node();
        n.id = pieces[1];
        n.value = pieces[3];
        return n;

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
            sp = (StringParser) Bin.fromString(context.getConfiguration().get("Bin.Mapper.Parser"));
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
            
        } catch (Exception ex) {
            System.err.println("Failed to parse " + s);
        }
    }
    
    
}

class BinReducer extends Reducer<Text, Node, NullWritable, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Node> nodes = new ArrayList<>();
        
        for (Node n : values) {
            for (int i = 0; i < nodes.size(); i++) {
                Node other = nodes.get(i);
                double similarity = Similarity(n.value, other.value);

                Edge e = new Edge(n, other, similarity);
                context.write(NullWritable.get(), new Text(e.toString()));
            }
            
            // We need to create a copy of n, as Hadoop will reuse the same
            // object, and simply overwrite the values
            // see Node.readFields(...)
            nodes.add(new Node(n));
        }
    }

    private double Similarity(String value1, String value2) {
        return jw_similarity(value1, value2);
    }

    public static double jw_similarity(String s1, String s2) {
        if (s1.equals(s2)) {
            return 1.0;
        }

        // ensure that s1 is shorter than or same length as s2
        if (s1.length() > s2.length()) {
            String tmp = s2;
            s2 = s1;
            s1 = tmp;
        }

    // (1) find the number of characters the two strings have in common.
        // note that matching characters can only be half the length of the
        // longer string apart.
        int maxdist = s2.length() / 2;
        int c = 0; // count of common characters
        int t = 0; // count of transpositions
        int prevpos = -1;
        for (int ix = 0; ix < s1.length(); ix++) {
            char ch = s1.charAt(ix);

            // now try to find it in s2
            for (int ix2 = Math.max(0, ix - maxdist);
                    ix2 < Math.min(s2.length(), ix + maxdist);
                    ix2++) {
                if (ch == s2.charAt(ix2)) {
                    c++; // we found a common character
                    if (prevpos != -1 && ix2 < prevpos) {
                        t++; // moved back before earlier 
                    }
                    prevpos = ix2;
                    break;
                }
            }
        }

    // we don't divide t by 2 because as far as we can tell, the above
        // code counts transpositions directly.
    // System.out.println("c: " + c);
        // System.out.println("t: " + t);
        // System.out.println("c/m: " + (c / (double) s1.length()));
        // System.out.println("c/n: " + (c / (double) s2.length()));
        // System.out.println("(c-t)/c: " + ((c - t) / (double) c));
        // we might have to give up right here
        if (c == 0) {
            return 0.0;
        }

        // first compute the score
        double score = ((c / (double) s1.length())
                + (c / (double) s2.length())
                + ((c - t) / (double) c)) / 3.0;

        // (2) common prefix modification
        int p = 0; // length of prefix
        int last = Math.min(4, s1.length());
        for (; p < last && s1.charAt(p) == s2.charAt(p); p++)
      ;

        score = score + ((p * (1 - score)) / 10);

    // (3) longer string adjustment
        // I'm confused about this part. Winkler's original source code includes
        // it, and Yancey's 2005 paper describes it. However, Winkler's list of
        // test cases in his 2006 paper does not include this modification. So
        // is this part of Jaro-Winkler, or is it not? Hard to say.
        //
        //   if (s1.length() >= 5 && // both strings at least 5 characters long
        //       c - p >= 2 && // at least two common characters besides prefix
        //       c - p >= ((s1.length() - p) / 2)) // fairly rich in common chars
        //     {
        //     System.out.println("ADJUSTED!");
        //     score = score + ((1 - score) * ((c - (p + 1)) /
        //                                     ((double) ((s1.length() + s2.length())
        //                                                - (2 * (p - 1))))));
        // }
    // (4) similar characters adjustment
        // the same holds for this as for (3) above.
        return score;
    }

}