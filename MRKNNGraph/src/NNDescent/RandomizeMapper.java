package NNDescent;

import MRKNNGraph.Node;
import NNCtph.DefaultStringParser;
import NNCtph.StringParser;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author tibo
 */
public class RandomizeMapper
        extends Mapper<LongWritable, Text, LongWritable, Node> {
        
        public int K = 10;
        public int reduce_tasks = 100;
        public Random rnd;
        StringParser sp;
        

        public RandomizeMapper() {
        }

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            rnd = new Random();
            sp = new DefaultStringParser();
        }
        
        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            
            try {
                Node node = sp.parse(value.toString());
                
                for (int i = 0; i < K; i++) {
                context.write(
                        new LongWritable(rnd.nextInt(reduce_tasks)), 
                        node);
                }
            } catch (Exception ex) {
                System.out.println("Could not parse " + value.toString());
                context.getCounter("NNDescent", "Failed parsing lines").increment(1);
            }
            
        }
    }
