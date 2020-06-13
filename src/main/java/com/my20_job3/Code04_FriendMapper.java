package com.my20_job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


//输入数据格式
//A	I,K,C,B,G,F,H,O,D,
//B	A,F,J,E,
//C	A,E,B,H,F,G,K,
//D	G,C,K,A,L,F,E,H,
//E	G,M,L,H,A,F,B,D,
//F	L,M,D,C,G,A,
//G	M,
//H	O,
//I	O,C,
//J	O,
//K	B,
//L	D,E,
//M	E,F,
//O	A,H,I,J,F,

public class Code04_FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取数据并切割，共同的好友
        String[] line = value.toString().split("\t");
        v.set(line[0]);


        //对粉丝排序并切分
        String[] fans = line[1].split(",");
        Arrays.sort(fans);

        for (int i = 0; i < fans.length; ++i) {
            for (int j = i + 1; j < fans.length; ++j) {

                //组合key
                k.set(fans[i] + "-" + fans[j]);
                context.write(k, v);
            }
        }
    }
}