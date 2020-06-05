package com.my10_combiner;

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
public class Code_04_Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Code_04_Driver.class);
        job.setMapperClass(Code_01_WordCountMapper.class);
        job.setReducerClass(Code_03_WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置预聚合Combiner
        job.setCombinerClass(Code_02_WordCountCombiner.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/10"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/10"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
