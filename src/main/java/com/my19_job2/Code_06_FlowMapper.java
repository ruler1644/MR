package com.my19_job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @Auther wu
 * @Date 2019/6/22  14:24
 */

//输入数据格式
//13470253144	180	180	360
//13509468723	7335	110349	117684

public class Code_06_FlowMapper extends Mapper<LongWritable, Text, Code_05_OrderFlowBean, Text> {

    //使用集合TreeMap作为存储数据的容器(按key有序)
    TreeMap<Code_05_OrderFlowBean, Text> flowMap = new TreeMap<>();
    Code_05_OrderFlowBean flowBean;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        flowBean = new Code_05_OrderFlowBean();
        Text v = new Text();


        String line = value.toString();
        String[] fields = line.split("\t");

        //封装数据
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);

        v.set(phoneNum);

        //向TreeMap中添加数据
        flowMap.put(flowBean, v);

        //限制TreeMap中的数据量，超过10条时，就删除一条
        if (flowMap.size() > 10) {

            //据升序还是降序，删除一条数据
            //flowMap.remove(flowMap.firstKey());
            flowMap.remove(flowMap.lastKey());
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //遍历Map集合输出数据
        Iterator<Code_05_OrderFlowBean> iterator = flowMap.keySet().iterator();

        while (iterator.hasNext()) {
            Code_05_OrderFlowBean k = iterator.next();
            context.write(k, flowMap.get(k));
        }
    }
}
