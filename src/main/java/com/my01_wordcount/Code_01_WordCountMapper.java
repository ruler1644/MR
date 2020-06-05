package com.my01_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/21  23:17
 */

/**
 * map阶段
 * KEYIN 输入数据key，这行内容开头在文件中的偏移量
 * VALUEIN 输入数据value，一行的内容
 * <p>
 * KEYOUT 输出数据key的类型
 * VALUEOUT 输出数据value的类型
 */
public class Code_01_WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    //每一个K-V调用一次map()
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
