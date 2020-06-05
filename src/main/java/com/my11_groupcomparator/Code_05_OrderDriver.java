package com.my11_groupcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/25  17:03
 */
public class Code_05_OrderDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_05_OrderDriver.class);
        job.setMapperClass(Code_02_OrderMapper.class);
        job.setReducerClass(Code_04_OrderReducer.class);


        //设置reduce端的分组
        job.setGroupingComparatorClass(Code_03_OrderGroupingComparator.class);

        job.setMapOutputValueClass(Code_01_OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Code_01_OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/11/"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/11/"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
