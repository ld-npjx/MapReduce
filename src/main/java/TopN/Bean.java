package TopN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    private String id;
    private Double price;
    public Bean(){}
    public Bean(String id, Double price) {
        this.id = id;
        this.price = price;
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public double getPrice() {
        return price;
    }
    public void setPrice(double price) {
        this.price = price;
    }
    @Override
    public int compareTo(Bean o) {
        int i = this.id.compareTo(o.getId());
        if(i==0) {
            i=this.price.compareTo(o.getPrice())*-1;
            return i;
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readUTF();
        this.price=dataInput.readDouble();
    }
}
