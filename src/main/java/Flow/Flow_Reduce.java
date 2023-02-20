package Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public class Flow_Reduce extends Reducer<Text, Flow_Bean,Text,Flow_Bean> {

    @Override
    protected void reduce(Text key, Iterable<Flow_Bean> values, Reducer<Text, Flow_Bean, Text, Flow_Bean>.Context context) throws IOException, InterruptedException {
        Flow_Bean bean = new Flow_Bean(0,0,0,0);
        Iterator<Flow_Bean> iterator = values.iterator();
        while(iterator.hasNext()){
            Flow_Bean bean_itr=iterator.next();
            bean.setUpFlow(bean.getUpFlow()+bean_itr.getUpFlow());
            bean.setDownFlow(bean.getDownFlow()+bean_itr.getDownFlow());
            bean.setUpCountFlow(bean.getUpCountFlow()+bean_itr.getUpCountFlow());
            bean.setDownCountFlow(bean.getDownCountFlow()+bean_itr.getDownCountFlow());
        }
        context.write(key,bean);
    }
}
