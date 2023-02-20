package cn;

import org.apache.commons.io.FileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class MapReduceMain extends Configured implements Tool {

    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //1.创建一个job对象
        Job wordCount = Job.getInstance(super.getConf(), "wordCount");

        //2.配置job任务对象
        wordCount.setInputFormatClass(TextInputFormat.class);
                //设置任务文件的读取地址
        //Path pathIn = new Path("hdfs://hadoop200:8020/data/word_test");
        //Local
        Path pathIn = new Path("file:///D:\\MR_test\\input");
        TextInputFormat.addInputPath(wordCount,pathIn);



        //3.指定map阶段的处理方式
        wordCount.setMapperClass(WordCountMap.class);
                //set key2 value2
        wordCount.setMapOutputKeyClass(Text.class);
        wordCount.setMapOutputValueClass(LongWritable.class);

        //4.5.6使用默认配置

        //7.指定reduce阶段的处理方式
        wordCount.setReducerClass(WordCountReduce.class);

                //set key3 value3
        wordCount.setOutputKeyClass(Text.class);
        wordCount.setOutputValueClass(LongWritable.class);


        //set output  Type&&address
        wordCount.setOutputFormatClass(TextOutputFormat.class);
                //判断该地址是否存在
        //Path pathOut = new Path("hdfs://hadoop200:8020/data/out_word");   //Yarn
        //local
        Path pathOut = new Path("file:///D:\\MR_test\\output");
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop200:8020"), new Configuration());
//        boolean bl1 = fileSystem.exists(pathOut);
//
//        //exist will delete
//        if(bl1)
//            fileSystem.delete(pathOut,true);//第二个参数表示是否递归删除

        //output inpath
        TextOutputFormat.setOutputPath(wordCount,pathOut);
        //判断是否执行成功
        boolean b = wordCount.waitForCompletion(true);

        return b ? 0:1;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new MapReduceMain(), args);
        System.exit(run);

    }
}
