package com.my05_nlineinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  0:05
 */
public class Code_03_Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code_03_Driver.class);

        //设置InputFormat为NLineInputFormat，每2行切一片
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 2);

        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_02_WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/5"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/5"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
