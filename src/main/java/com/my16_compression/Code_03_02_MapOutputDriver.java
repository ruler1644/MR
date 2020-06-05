package com.my16_compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  0:05
 */
public class Code_03_02_MapOutputDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //开启map端输出压缩，设置map端输出压缩方式
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code_03_02_MapOutputDriver.class);
        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_02_WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/16/2"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/16/2"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
