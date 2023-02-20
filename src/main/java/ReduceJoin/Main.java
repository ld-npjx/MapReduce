package ReduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sort.SortMap;
import sort.SortReduce;
import sort.TheSort;

public class Main extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "RuduceJoin");
        job.setJarByClass(Main.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop200:8020/input/"));


        job.setMapperClass(Map.class);
        //set k2 v2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

         job.setReducerClass(Reduce.class);
         job.setOutputValueClass(Text.class);
         job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop200:8020/output_ReduceJoin"));

        boolean bl=job.waitForCompletion(true);
        return bl ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();
        int run = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(run);
    }
}
