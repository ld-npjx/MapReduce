package Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Flow_Partitioner extends Partitioner<Text,Flow_Bean> {

    @Override
    public int getPartition(Text text, Flow_Bean flow_bean, int i) {
        String string = text.toString();
        if(string.startsWith("136"))
            return 1;
        if(string.startsWith("133"))
            return 2;
        if(string.startsWith("135"))
            return 3;
        return 4;
    }
}
