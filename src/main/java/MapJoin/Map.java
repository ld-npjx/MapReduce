package MapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class Map extends Mapper<LongWritable, Text,Text,Text> {

    private HashMap<String, String> map = new HashMap<>();
    //将分布式缓存的小表数据读取到本地的Map集合中（只需要进行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.获取分布式缓存文件列表
        URI[] cacheFiles = context.getCacheFiles();
        //2.获取指定的分布式缓存的文件系统(FileSystem)
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
        //3.获取文件的输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        //inputStream读取的是文件中的字节流，要将其转化为字符流，然后逐行得到数据
        //4.读取文件系统，将数据存入Map
            //4.1将字节输入流转为字符缓冲流FSDataInputStream----->>BufferedReader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            //4.2读取小文件内容，以行为单位，并将读取的数据存入Map集合
        String Line=null;
        while((Line=bufferedReader.readLine())!=null){
            String[] s = Line.split(" ");
            map.put(s[0],Line);
        }
        bufferedReader.close();
        fileSystem.close();

    }
    //对大表进行业务逻辑处理 ， 并且实现大小表的join
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        String Key2 = s[2];//key2

        String productLine=map.get(Key2);
        String valueLine = productLine + '\t' + value.toString();

        context.write(new Text(Key2),new Text(valueLine));
    }
}
