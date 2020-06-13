package com.my18_job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Code02_OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        //统计词频
        for (IntWritable val : values) {
            sum += val.get();
        }
        System.out.println("**************" + total);
        total.set(sum);
        context.write(key, total);
    }
}
