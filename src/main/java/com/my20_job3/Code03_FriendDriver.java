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
public class Code03_FriendDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code03_FriendDriver.class);
        job.setMapperClass(Code01_FriendMapper.class);
        job.setReducerClass(Code02_FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/job3"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/job3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
