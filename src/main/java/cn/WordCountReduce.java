package cn;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class WordCountReduce extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        Long count = new Long(0);
        for (LongWritable value : values) {
            count+=value.get();

        }
        LongWritable outPutValue=new LongWritable(count);
        context.write(key,outPutValue);
    }
}


