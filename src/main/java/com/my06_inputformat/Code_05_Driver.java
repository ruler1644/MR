package com.my06_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/24  10:37
 */
public class Code_05_Driver {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_05_Driver.class);
        job.setMapperClass(Code_03_Mapper.class);
        job.setReducerClass(Code_04_SequenceFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置输入的InputFormat
        job.setInputFormatClass(Code_01_MyInputFormat.class);
        //设置输出的OutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/6"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/6"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}