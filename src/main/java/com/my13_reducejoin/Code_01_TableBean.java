package com.my13_reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/30  17:52
 */

//自定义Bean，包含两张表的所有字段
public class Code_01_TableBean implements WritableComparable<Code_01_TableBean> {


    private String id;
    private String pid;
    private int amount;
    private String pname;

    public Code_01_TableBean() {

    }

    public Code_01_TableBean(String id, String pid, int amount, String pname) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
    }

    //排序规则：先按pid排序，相同时再按pname排序。有pname的在前
    @Override
    public int compareTo(Code_01_TableBean o) {
        int res = this.pid.compareTo(o.pid);
        if (res == 0) {
            return o.pname.compareTo(this.pname);
        }
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pname = in.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }
}
