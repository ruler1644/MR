package com.my15_ETLAndCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/7/1  19:53
 */
public class Code_02_LogDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_02_LogDriver.class);
        job.setMapperClass(Code_01_LogMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //ETL不需要使用Reduce
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/15"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/15"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
