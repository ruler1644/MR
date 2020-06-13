package com.my19_job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/22  14:25
 */
public class Code_03_FlowReducer extends Reducer<Text, Code_01_FlowBean, Text, Code_01_FlowBean> {

    Code_01_FlowBean flow2 = new Code_01_FlowBean();

    @Override
    protected void reduce(Text key, Iterable<Code_01_FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        //13846544121 264 470
        //13846544121 464 345
        //累加上行流量和下行流量，得到总流量
        for (Code_01_FlowBean val : values) {
            sum_upFlow += val.getUpFlow();
            sum_downFlow += val.getDownFlow();
        }

        flow2.set(sum_upFlow, sum_downFlow);
        context.write(key, flow2);
    }
}
