package com.my04_keyvaluetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/25  23:54
 */
public class Code03_KVDriver {
    public static void main(String[] args) throws Exception {

        //设置分隔符号
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code03_KVDriver.class);
        job.setMapperClass(Code01_KVMapper.class);
        job.setReducerClass(COde02_KVReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置InputFormat的类型
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\delete\\input\\4"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\delete\\output\\4"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
