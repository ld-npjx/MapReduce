package fileOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class fileOutputMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(),"FileOutput");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop200:8020/input/MyFileOut"));

        job.setMapperClass(fileOutputMap.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

//没有Reduce就不需要输出这些
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);

        //设置输出类和输出路径
        job.setOutputFormatClass(MyFileOutputFormat.class);
        //这个是元数据的保存地址，和MyFileOutputFormat中保存的数据不冲突
        MyFileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop200:8020/MyFileOutput"));

        boolean b = job.waitForCompletion(true);
        return b ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new fileOutputMain(), args);
        System.exit(run);
    }
}
