package com.my09_partitionerorder;

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
public class Code_05_FlowDriver {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_05_FlowDriver.class);
        job.setMapperClass(Code_02_FlowMapper.class);
        job.setReducerClass(Code_04_FlowReducer.class);

        job.setMapOutputKeyClass(Code_01_FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Code_01_FlowBean.class);

        //设置分区参数
        job.setPartitionerClass(Code_03_MyPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/9"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/9"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
