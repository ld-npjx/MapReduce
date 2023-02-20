package sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMap extends Mapper<LongWritable, Text,TheSort,NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TheSort, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split("    ");
        //注意linux(6)和window(4)中tab键表示的不一样
        TheSort theSort = new TheSort(str[0],Integer.valueOf(str[1]));

        context.write(theSort,NullWritable.get());
    }
}
