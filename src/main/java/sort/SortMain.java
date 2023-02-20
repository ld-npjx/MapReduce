package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortMain extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        Job job=Job.getInstance(super.getConf(),"SortText");

        job.setJarByClass(SortMain.class);
        //记得设置jobjar通过什么运行

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop200:8020/input/sort/sortTest"));


        job.setMapperClass(SortMap.class);
        //set k2 v2
        job.setMapOutputKeyClass(TheSort.class);
        job.setMapOutputValueClass(NullWritable.class);



        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(TheSort.class);
        job.setOutputValueClass(NullWritable.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop200:8020/output_sort"));

        boolean bl=job.waitForCompletion(true);
        return bl ? 0:1;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new SortMain(), args);
        System.exit(run);
    }


}
