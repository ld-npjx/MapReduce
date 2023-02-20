package MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class Main extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "Map_Join");

        //将小表放入分布式缓存中
        job.addCacheArchive(new URI("hdfs://hadoop200:8020/input/little_test"));
        //下面这种方法也行，但是不推荐了
        //DistributedCache.addCacheFile(new URI("hdfs://hadoop200:8020/input/little_test"),super.getConf());

        //SET INPUT FILE PATH
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop200:8020/input/big_test"));
        //指定大表地址

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop200:8020/Map_join"));

        boolean b = job.waitForCompletion(true);
        return b? 0:1 ;
    }

    public static void main(String[] args) throws Exception {
        Configuration configured = new Configuration();
        int I = ToolRunner.run(configured, new Main(), args);
        System.exit(I);
    }
}
