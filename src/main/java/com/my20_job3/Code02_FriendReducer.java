package com.my20_job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Code02_FriendReducer extends Reducer<Text, Text, Text, Text> {

    Text val = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //将数据放在同一行
        StringBuilder sb = new StringBuilder();

        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }

        val.set(sb.toString());
        context.write(key, val);
    }
}
