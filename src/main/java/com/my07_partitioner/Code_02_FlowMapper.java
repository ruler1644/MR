package com.my07_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/22  14:24
 */
public class Code_02_FlowMapper extends Mapper<LongWritable, Text, Text, Code_01_FlowBean> {
    Text k = new Text();
    Code_01_FlowBean v = new Code_01_FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        k.set(fields[1]);

        //13846544121	192.196.100.2			264	0	200
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        context.write(k,v);
    }
}
