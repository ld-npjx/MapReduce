package fileOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWrite extends RecordWriter<Text, Text> {
    private FSDataOutputStream GoodDataOutputStream=null;
    private FSDataOutputStream BadDataOutputStream=null;

    public MyRecordWrite(){
    }
    public MyRecordWrite(FSDataOutputStream goodDataOutputStream, FSDataOutputStream badDataOutputStream) {
        GoodDataOutputStream = goodDataOutputStream;
        BadDataOutputStream = badDataOutputStream;
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        if (Integer.parseInt(key.toString()) >= 1) {
            GoodDataOutputStream.write(value.toString().getBytes());
            GoodDataOutputStream.write("\r\n".getBytes());
        }else {
            BadDataOutputStream.write(value.toString().getBytes());
            BadDataOutputStream.write("\r\n".getBytes());
        }
    }
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(GoodDataOutputStream);
        IOUtils.closeStream(BadDataOutputStream);
    }
}
