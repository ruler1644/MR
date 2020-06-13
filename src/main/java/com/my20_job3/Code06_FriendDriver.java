package com.my20_job3;

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
public class Code06_FriendDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code06_FriendDriver.class);
        job.setMapperClass(Code04_FriendMapper.class);
        job.setReducerClass(Code05_FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/output/job3"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/job31"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
