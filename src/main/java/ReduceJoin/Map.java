package ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import static jdk.jpackage.internal.Arguments.CLIOptions.context;

public class Map extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    //通过输入路径来判断是哪个文件
        String[] Str = value.toString().split(",");
//        FileSplit split= (FileSplit) context.getInputSplit();
//        String name = split.getPath().getName();
//        if(name.equals("ReduceJoin.txt"))
//            context.write(new Text(Str[2]),value);
//        else
//            context.write(new Text(Str[3]),value);


  //通过文件内容来判断文件来源
        if(Str[5].startsWith("P"))
            context.write(new Text(Str[2]),value);
        else
            context.write(new Text(Str[3]),value);
    }
}
