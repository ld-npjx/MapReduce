package fileOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFileOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());

        FSDataOutputStream GoodDataOutputStream = fileSystem.create(new Path("hdfs://hadoop200:8020/output_good"));
        FSDataOutputStream BadDataOutputStream = fileSystem.create(new Path("hdfs://hadoop200:8020/output_bad"));


        MyRecordWrite myRecordWrite = new MyRecordWrite(GoodDataOutputStream,BadDataOutputStream);
        return myRecordWrite;
    }
}
