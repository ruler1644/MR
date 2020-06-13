package com.my19_job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/22  14:24
 */
public class Code_02_FlowMapper extends Mapper<LongWritable, Text, Text, Code_01_FlowBean> {

    Text phoneNum = new Text();
    Code_01_FlowBean flow = new Code_01_FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        phoneNum.set(fields[0]);

        //13470253144	180	180	360
        //13509468723	7335	110349	117684
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        context.write(phoneNum, flow);
    }
}
