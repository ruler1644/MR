package com.my18_job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Code05_TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //将数据拼接成一行
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString() + "\t");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
