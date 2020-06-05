package com.my13_reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/30  18:08
 */
public class Code_05_TableDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_05_TableDriver.class);
        job.setMapperClass(Code_02_TableMapper.class);
        job.setReducerClass(Code_04_TableReducer.class);

        job.setMapOutputKeyClass(Code_01_TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Code_01_TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //分组器
        job.setGroupingComparatorClass(Code_03_MyComparator.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/13"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/13"));

        boolean b = job.waitForCompletion(true);
        System.exit(b == true ? 0 : 1);
    }
}
