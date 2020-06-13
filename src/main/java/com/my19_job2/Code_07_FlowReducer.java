package com.my19_job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @Auther wu
 * @Date 2019/6/22  14:25
 */
public class Code_07_FlowReducer extends Reducer<Code_05_OrderFlowBean, Text, Text, Code_05_OrderFlowBean> {

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    TreeMap<Code_05_OrderFlowBean, Text> flowMap = new TreeMap<Code_05_OrderFlowBean, Text>();


    @Override
    protected void reduce(Code_05_OrderFlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        /*//全部遍历
        for (Text phoneNum : values) {
            context.write(phoneNum, key);
        }*/


        /*//取出Top 10
        //思路是错误的，它是取出一个流量，对应的前10的记录。而不是流量总的前10
        //如240对应13条记录，取出10条。若对应只有一条记录，则会全部取出
        Iterator<Text> iterator = values.iterator();
        for (int i = 0; i < 10; i++) {
            if (iterator.hasNext()) {
                Text next = iterator.next();
                context.write(next, key);
            }
        }*/


        for (Text val : values) {
            Code_05_OrderFlowBean flow2 = new Code_05_OrderFlowBean();
            flow2.set(key.getUpFlow(), key.getDownFlow());

            flowMap.put(flow2, val);

            //限制TreeMap数据量，超过10条就删除掉流量最小的一条数据
            if (flowMap.size() > 10) {

                //flowMap.remove(flowMap.firstKey());
                flowMap.remove(flowMap.lastKey());
            }
        }

    }

    @Override
    protected void cleanup(Reducer<Code_05_OrderFlowBean, Text, Text, Code_05_OrderFlowBean>.Context context)
            throws IOException, InterruptedException {

        //遍历集合，输出数据
        Iterator<Code_05_OrderFlowBean> it = flowMap.keySet().iterator();

        while (it.hasNext()) {

            Code_05_OrderFlowBean v = it.next();

            context.write(new Text(flowMap.get(v)), v);
        }
    }
}
