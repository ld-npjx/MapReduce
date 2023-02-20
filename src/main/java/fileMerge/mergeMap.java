package fileMerge;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class mergeMap extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        //k2为文件名
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        context.write(new Text(name),value);
    }
}
