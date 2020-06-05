package com.my16_compression;

import com.my01_wordcount.Code_01_WordCountMapper;
import com.my01_wordcount.Code_02_WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  0:05
 */
public class Code_03_03_ReducerOutPutDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code_03_03_ReducerOutPutDriver.class);
        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_02_WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //开启reduce端输出压缩，设置压缩的方式
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/16/3"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/16/3"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
