package fileMerge;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

public class myInputFormat extends FileInputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        myRecodeReader myRecodeReader = new myRecodeReader();
        myRecodeReader.initialize(inputSplit,taskAttemptContext);
        return myRecodeReader;
    }
}
