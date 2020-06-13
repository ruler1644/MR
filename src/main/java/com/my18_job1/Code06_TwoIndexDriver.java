package com.my18_job1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Code06_TwoIndexDriver {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Code06_TwoIndexDriver.class);
        job.setMapperClass(Code04_TwoIndexMapper.class);
        job.setReducerClass(Code05_TwoIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:/delete/output/job1"));
        FileOutputFormat.setOutputPath(job, new Path("D:/delete/output/job12"));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
