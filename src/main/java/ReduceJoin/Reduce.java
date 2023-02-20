package ReduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String beginning="";
        String mainStr="";
        for(Text value:values){
            String str = value.toString();
            if(str.startsWith("pld"))
                beginning=str;
            else
                mainStr+=(str+'\t');
        }
        context.write(key,new Text(beginning+'\t'+mainStr));
    }
}
