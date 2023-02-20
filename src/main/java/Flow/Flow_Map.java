package Flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Flow_Map extends Mapper<LongWritable, Text,Text,Flow_Bean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Flow_Bean>.Context context) throws IOException, InterruptedException {
        String[] text=value.toString().split(" ");
        Flow_Bean flow_bean = new Flow_Bean(Integer.valueOf(text[0]), Integer.valueOf(text[1]), Integer.valueOf(text[2]), Integer.valueOf(text[3]));
        context.write(new Text(text[4]),flow_bean);
    }
}
