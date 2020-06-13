package com.my20_job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//输入数据格式
//A:B,C,D,F,E,O

//输出数据格式
//A	I,K,C,B,G,F,H,O,D,
//B	A,F,J,E,
//C	A,E,B,H,F,G,K,
//D	G,C,K,A,L,F,E,H,

public class Code01_FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取数据并切割
        String[] fields = value.toString().split(":");
        String[] friends = fields[1].split(",");
        v.set(fields[0]);

        for (String friend : friends) {
            k.set(friend);
            context.write(k, v);
        }
    }
}