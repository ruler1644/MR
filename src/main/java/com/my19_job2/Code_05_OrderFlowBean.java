package com.my19_job2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/22  12:04
 */
public class Code_05_OrderFlowBean implements WritableComparable<Code_05_OrderFlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public Code_05_OrderFlowBean() {
    }


    public Code_05_OrderFlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    //序列化方法，对象--->字节序列
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    //反序列化方法，字节序列--->对象
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void set(long upFlow2, long downFlow2) {
        this.upFlow = upFlow2;
        this.downFlow = downFlow2;
        this.sumFlow = upFlow2 + downFlow2;
    }

    //按总的流量排序
    @Override
    public int compareTo(Code_05_OrderFlowBean o) {
        return Long.compare(o.sumFlow, this.getSumFlow());
    }
}
