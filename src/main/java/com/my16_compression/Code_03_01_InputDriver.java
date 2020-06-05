package com.my16_compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  0:05
 */
public class Code_03_01_InputDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code_03_01_InputDriver.class);
        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_02_WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //输入端压缩
        //hadoop会根据文件的扩展名，自动决定采取哪种压缩格式，不需要任何配置
        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/16/1/word.txt.gz"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/16/1"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
