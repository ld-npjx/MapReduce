package fileMerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class myRecodeReader extends RecordReader<NullWritable, BytesWritable> {
    private int length=0;
    private Configuration configuration=null;
    private FileSplit fileSplit=null;
    private BytesWritable bytesWritable=null;
    private boolean end=false;

    //source
    private FileSystem fileSystem=null;
    private FSDataInputStream fsInputStream=null;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        length=(int)inputSplit.getLength();
        configuration = taskAttemptContext.getConfiguration();
        fileSplit=(FileSplit)inputSplit;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!end) {
            byte[] bytes = new byte[length];

            //获取源文件系统，（fileSystem
            Path path = fileSplit.getPath();
            fileSystem = path.getFileSystem(configuration);
            fsInputStream = fileSystem.open(path);//得到源文件字节输入流

            //将文件字节流存到字节数组中
            IOUtils.readFully(fsInputStream, bytes, 0, length);

            //封装types
            bytesWritable = new BytesWritable();
            bytesWritable.set(bytes,0,length);

            end=true;

            return end;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        fsInputStream.close();
        fileSystem.close();
    }
}
