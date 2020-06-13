package com.my08_order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/27  12:56
 */
public class Code_05_FlowBean implements WritableComparable<Code_05_FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public Code_05_FlowBean() {
    }

    public Code_05_FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //按照总流量进行排序，降序
    @Override
    public int compareTo(Code_05_FlowBean o) {
        return Long.compare(o.sumFlow, this.getSumFlow());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
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
}
