package com.my04_keyvaluetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  23:41
 */
public class COde02_KVReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable val:values){
            sum += val.get();
        }

        v.set(sum);
        context.write(key,v);
    }
}
