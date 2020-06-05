package com.my09_partitionerorder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  16:25
 */


//分区内排序，按照每个手机号总流量内部排序
public class Code_02_FlowMapper extends Mapper<LongWritable, Text, Code_01_FlowBean, Text> {

    Code_01_FlowBean k = new Code_01_FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //13736230513	4962	49362	54324
        String line = value.toString();
        String[] fields = line.split("\t");
        v.set(fields[0]);

        k.setUpFlow(Long.parseLong(fields[1]));
        k.setDownFlow(Long.parseLong(fields[2]));
        k.setSumFlow(Long.parseLong(fields[3]));

        context.write(k, v);
    }
}
