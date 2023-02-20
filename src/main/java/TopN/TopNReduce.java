package TopN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReduce extends Reducer<Bean, Text,Text, NullWritable> {
    @Override
    protected void reduce(Bean key, Iterable<Text> values, Reducer<Bean, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        int i=0;
        for (Text value:values){
            i++;
            String[] s = value.toString().split(" ");
            context.write(new Text("id:"+s[0]+'\t'+"price:"+s[3]),NullWritable.get());
            if(i>=1)  //只输出每组的第一个
                break;
        }
    }
}
