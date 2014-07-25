package info.debatty.hadoop.graphs.NNDescent;

import info.debatty.graphs.Node;
import info.debatty.hadoop.graphs.NodeWritable;
import info.debatty.hadoop.graphs.DefaultStringParser;
import info.debatty.hadoop.graphs.StringParserInterface;
import info.debatty.hadoop.util.ObjectSerialize;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author tibo
 */
public class RandomizeMapper
        extends Mapper<LongWritable, Text, LongWritable, NodeWritable> {
        
        public int K = 10;
        public int reduce_tasks = 100;
        public Random rnd;
        StringParserInterface sp;
        

        public RandomizeMapper() {
        }

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            rnd = new Random();
            
            try {
                sp = (StringParserInterface) ObjectSerialize.unserialize(context.getConfiguration().get(NNDescent.KEY_PARSER));
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(RandomizeMapper.class.getName()).log(Level.SEVERE, null, ex);
                System.err.println("Could not load string parser");
                System.exit(1);
            }
        }
        
        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            
            try {
                Node node = sp.parse(value.toString());
                
                for (int i = 0; i < K; i++) {
                context.write(
                        new LongWritable(rnd.nextInt(reduce_tasks)), 
                        new NodeWritable(node));
                }
            } catch (Exception ex) {
                System.out.println("Could not parse " + value.toString());
                context.getCounter("NNDescent", "Failed parsing lines").increment(1);
            }
            
        }
    }
