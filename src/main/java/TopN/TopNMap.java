package TopN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMap extends Mapper<LongWritable, Text,Bean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Bean, Text>.Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        Bean bean = new Bean(s[0], Double.valueOf(s[3]));
        context.write(bean,value);
    }
}
