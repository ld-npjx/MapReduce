package TopN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNpartition  extends Partitioner<Bean, Text> {

    @Override
    public int getPartition(Bean bean, Text text, int i) {
        return (bean.getId().hashCode() & 2147483647) %i;
    }
}
