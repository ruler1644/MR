package com.my02_writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  14:25
 */
public class Code_04_FlowDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_04_FlowDriver.class);
        job.setMapperClass(Code_02_FlowMapper.class);
        job.setReducerClass(Code_03_FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Code_01_FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Code_01_FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/2"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
