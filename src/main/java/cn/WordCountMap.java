package cn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text,Text,LongWritable>{
    //word count k1 v1

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(" ");

        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        for (String string : strings) {
            text.set(string);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }

}
