package com.my02_writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/22  14:24
 */
public class Code_02_FlowMapper extends Mapper<LongWritable, Text, Text, Code_01_FlowBean> {
    Text phone = new Text();
    Code_01_FlowBean flow = new Code_01_FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        phone.set(fields[1]);

        //3 13846544121	192.196.100.2			264	0	200
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        context.write(phone, flow);
    }
}
