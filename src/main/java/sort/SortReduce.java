package sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<TheSort, NullWritable,TheSort, NullWritable> {
    @Override
    protected void reduce(TheSort key, Iterable<NullWritable> values, Reducer<TheSort, NullWritable, TheSort, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
