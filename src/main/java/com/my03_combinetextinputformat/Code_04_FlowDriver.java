package com.my03_combinetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther wu
 * @Date 2019/6/22  14:25
 */
public class Code_04_FlowDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Code_04_FlowDriver.class);
        job.setMapperClass(Code_02_FlowMapper.class);
        job.setReducerClass(Code_03_FlowReducer.class);

        //使用CombineTextInputFormat，处理小文件
        //设置虚拟存储切片最大值
        //四个小文件，但是只形成一个切片
        //number of splits:1
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        job.setInputFormatClass(CombineTextInputFormat.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Code_01_FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Code_01_FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/3"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
