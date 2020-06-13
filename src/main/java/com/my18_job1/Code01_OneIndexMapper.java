package com.my18_job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//倒排索引(多Job串联案例)

//输入数据格式
//atgugu pingping
//atgugu ss
//atgugu ss

//atgugu pingping
//atgugu pingping
//pingping ss

//atgugu
//atgugu pingping


//输出数据
//atguigu --a.txt	 3
//atguigu --b.txt	 2
//atguigu --c.txt	 2
//pingping--a.txt	 1
//pingping--b.txt	 3
//pingping--c.txt	 1
//ss      --a.txt	 2
//ss      --b.txt	 1
//ss      --c.txt	 1
public class Code01_OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String fileName;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取文件名
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit1 = (FileSplit) inputSplit;
        fileName = fileSplit1.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //按空格切分一行数据，每个单词拼上文件名作为key
        String[] words = value.toString().split(" ");
        for (String word : words) {
            k.set(word + "--" + fileName);
            context.write(k, v);
        }
    }
}
