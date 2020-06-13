package com.my18_job1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Code03_OneIndexDriver {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Code03_OneIndexDriver.class);
        job.setMapperClass(Code01_OneIndexMapper.class);
        job.setReducerClass(Code02_OneIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:/delete/input/job1"));
        FileOutputFormat.setOutputPath(job, new Path("D:/delete/output/job1"));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
