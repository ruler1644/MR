package com.my16_compression;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/21  23:17
 */

public class Code_01_WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        System.out.println(key.toString() + "==========================");

        //获取一行数据，切割成单词
        String line = value.toString();
        String[] words = line.split(" ");

        //循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }

}
