package com.my01_wordcount;

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
public class Code_03_Driver {
    public static void main(String[] args) throws Exception {

        // 1 获取job实例对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置类路径
        job.setJarByClass(Code_03_Driver.class);

        // 3 关联mapper和reducer类
        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_02_WordCountReducer.class);

        // 4 设置mapper阶段，输出数据key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/1"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/1"));

        // 7 提交任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
