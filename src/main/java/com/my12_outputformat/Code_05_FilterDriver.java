package com.my12_outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/30  16:16
 */
public class Code_05_FilterDriver {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_05_FilterDriver.class);
        job.setMapperClass(Code_01_FilterMapper.class);
        job.setReducerClass(Code_02_FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输出OutPutFormat
        job.setOutputFormatClass(Code_03_MyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/12"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/12"));

        boolean b = job.waitForCompletion(true);
        System.exit(b == true ? 0 : 1);
    }
}
