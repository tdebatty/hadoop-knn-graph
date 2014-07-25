package info.debatty.hadoop.CartesianProduct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author tibo
 */
public class CartesianTextInputFormat extends FileInputFormat<Text, Text>{
    public static final String LEFT_INPUT_PATH = "cartesiancextinputformat.left.path";
    public static final String RIGHT_INPUT_PATH = "cartesiancextinputformat.right.path";

    public static void setLeftInputhPath(Job job, String in) {
        job.getConfiguration().set(LEFT_INPUT_PATH, in);
    }

    public static void setRightInputhPath(Job job, String in) {
        job.getConfiguration().set(RIGHT_INPUT_PATH, in);
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        String left_input_path = job.getConfiguration().get(LEFT_INPUT_PATH);
        String right_input_path = job.getConfiguration().get(RIGHT_INPUT_PATH);
        
        ArrayList splits = new ArrayList();
        
        Job new_job = new Job(job.getConfiguration());
        
        TextInputFormat tif = new TextInputFormat();
        
        TextInputFormat.setInputPaths(new_job, left_input_path);
        List<InputSplit> left_splits = tif.getSplits(new_job);
        
        TextInputFormat.setInputPaths(new_job,right_input_path);
        List<InputSplit> right_splits = tif.getSplits(new_job);
        
        for (InputSplit left : left_splits) {
            for (InputSplit right : right_splits) {
                try {
                    CompositeInputSplit cis = new CompositeInputSplit(2);
                    cis.add(left);
                    cis.add(right);
                    splits.add(cis);
                } catch (Exception ex) {
                    System.out.println("Could not create composite split :(");
                }
            }
        }
        //System.out.println("Created " + splits.size() + " splits!");
        return splits;
    }
    
    
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit is, TaskAttemptContext tac)
            throws IOException, InterruptedException {
        return new CartesianTextRecordReader();
    }
    
    
}
