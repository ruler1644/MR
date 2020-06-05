package com.my15_ETLAndCounter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/7/1  19:54
 */

//数据清洗 + 计数器应用
//去除字段数目小于11的日志
public class Code_01_LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Counter pass;
    private Counter fail;
    private Counter total;

    //设置计数器
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //计数器组名，计数器名的方式，获取计数器
        pass = context.getCounter("ETL", "pass");
        fail = context.getCounter("ETL", "fail");
        total = context.getCounter("ETL", "total");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行，总的计数器值加1
        String[] fields = value.toString().split(" ");
        total.increment(1);

        //字段数目大于11，可以写出
        if (fields.length > 11) {
            pass.increment(1);
            context.write(value, NullWritable.get());
        } else {
            fail.increment(1);
        }
    }
}
