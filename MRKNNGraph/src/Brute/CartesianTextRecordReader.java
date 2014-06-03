package Brute;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author tibo
 */
class CartesianTextRecordReader extends RecordReader<Text, Text> {
    private CompositeInputSplit input_split;
    private TaskAttemptContext context;
    
    RecordReader<LongWritable, Text> left_record_reader;
    RecordReader<LongWritable, Text> right_record_reader;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext context) throws IOException, InterruptedException {
        this.input_split = (CompositeInputSplit) is;
        this.context = context;
        
        TextInputFormat text_input_format = new TextInputFormat();
        left_record_reader = text_input_format.createRecordReader(input_split.get(0), context);
        left_record_reader.initialize(input_split.get(0), context);
        left_record_reader.nextKeyValue();
        
        right_record_reader = text_input_format.createRecordReader(input_split.get(1), context);
        right_record_reader.initialize(input_split.get(1), context);
        
        //System.out.println(left_record_reader.getCurrentValue().toString());
        
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (right_record_reader.nextKeyValue()) {
            return true;
        }
            
        // Right reader is at the end => try to move left reader
        if (!left_record_reader.nextKeyValue()) {
            // Left is also at the end 
            return false;
        }

        // Reset right record reader
        // System.out.println(left_record_reader.getProgress());
        right_record_reader.close();
        TextInputFormat text_input_format = new TextInputFormat();
        right_record_reader = text_input_format.createRecordReader(input_split.get(1), context);
        right_record_reader.initialize(input_split.get(1), context);
        right_record_reader.nextKeyValue();
        return true;
        
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return left_record_reader.getCurrentValue();
        
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return right_record_reader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return left_record_reader.getProgress();
        
    }

    @Override
    public void close() throws IOException {
        left_record_reader.close();
        right_record_reader.close();
        
    }
    
}
