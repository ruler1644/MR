package com.my18_job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//倒排索引

//输入数据
//atguigu --a.txt	 3
//atguigu --b.txt	 2
//atguigu --c.txt	 2
//pingping--a.txt	 1
//pingping--b.txt	 3
//pingping--c.txt	 1
//ss      --a.txt	 2
//ss      --b.txt	 1
//ss      --c.txt	 1

//输出数据
//atguigu   c.txt-->2 b.txt-->2	a.txt-->3
//pingping	c.txt-->1 b.txt-->3	a.txt-->1
//ss	    c.txt-->1 b.txt-->1	a.txt-->1

public class Code04_TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String[] split = value.toString().split("\t");
        String[] fields = split[0].split("--");


        //重组k-v形式如下：< 单词，文件名--数量 >
        k.set(fields[0]);
        v.set(fields[1] + "-->" + split[1]);
        context.write(k, v);
    }
}
