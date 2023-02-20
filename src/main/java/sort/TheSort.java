package sort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TheSort implements WritableComparable<TheSort> {

    private String word;
    private int num;


    public TheSort(String word, int num) {
        this.word = word;
        this.num = num;
    }

    //记得定义无参构造  MapReduce需要

    public TheSort() {
    }

    public void setWord(String word) {
        this.word = word;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getWord() {
        return word;
    }

    public int getNum() {
        return num;
    }

    @Override
    public String toString() {
        return  word+"/t"+num;
    }

    @Override
    public int compareTo(TheSort theSort) {
        int result = this.word.compareTo(theSort.word);
        if(result==0)
            return this.num- theSort.num;
        return result;
    }


    //实现序列化  转化为字节流
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);//String type
        dataOutput.writeInt(num);//int type
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word=dataInput.readUTF();
        this.num=dataInput.readInt();
    }
}
