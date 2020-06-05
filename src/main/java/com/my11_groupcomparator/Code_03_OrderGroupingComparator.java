package com.my11_groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Auther wu
 * @Date 2019/6/25  16:40
 */

//在ReduceTask的Reduce方法之前，执行GroupingComparator，对从Map端拉取过来的数据再次排序
//只要订单id相同就进入同一个Reducer
public class Code_03_OrderGroupingComparator extends WritableComparator {

    //调用父类构造器，MR的序列化，不单单在网络传输数据时使用，在MR各模块之间传输数据时也会用到序列化
    //每个模块处理数据时，先反序列化，还原出对象，再使用对象
    public Code_03_OrderGroupingComparator() {
        super(Code_01_OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Code_01_OrderBean orderBean1 = (Code_01_OrderBean) a;
        Code_01_OrderBean orderBean2 = (Code_01_OrderBean) b;
        return orderBean1.getOrderId().compareTo(orderBean2.getOrderId());
    }
}
