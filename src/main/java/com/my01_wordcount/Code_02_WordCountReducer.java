package com.my01_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @Auther wu
 * @Date 2019/6/19  16:13
 */

/**
 * Reduce的输入就是Mapper的输出
 * KEYIN
 * VALUEIN
 * KEYOUT
 * VALUEOUT
 */

public class Code_02_WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable total = new IntWritable();

    //每一组K-V调用一次reduce()
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        total.set(sum);
        context.write(key, total);
    }
}

