package TopN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNComparator extends WritableComparator {
    public TopNComparator(){
        super(Bean.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b) {

        Bean First=(Bean)a;
        Bean Second=(Bean)b;

        return First.getId().compareTo(Second.getId());
    }
}
