package com.my19_job2;

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
public class Code_08_FlowDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_08_FlowDriver.class);
        job.setMapperClass(Code_06_FlowMapper.class);
        job.setReducerClass(Code_07_FlowReducer.class);

        job.setMapOutputKeyClass(Code_05_OrderFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Code_05_OrderFlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/job2"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/job2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
