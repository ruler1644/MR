package com.my04_keyvaluetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  23:37
 */

//统计输入文件中每一行的第一个单词相同的行数
public class Code01_KVMapper extends Mapper<Text, Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        context.write(key, v);
    }
}
