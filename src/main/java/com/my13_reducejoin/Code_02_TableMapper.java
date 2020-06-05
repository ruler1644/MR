package com.my13_reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/30  18:08
 */

//将关联条件作为Map输出的key，将两表满足Join条件的数据并携带数据所来源的文件信息，发往同一个ReduceTask，在Reduce中进行数据的串联
public class Code_02_TableMapper extends Mapper<LongWritable, Text, Code_01_TableBean, NullWritable> {

    String filename;
    Code_01_TableBean tableBean = new Code_01_TableBean();

    //通过切片，获取文件的名字
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        filename = fileSplit.getPath().getName();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //order表
        //1001	01	1
        //1004	01	4

        //pd表
        //01	小米
        String line = value.toString();
        String[] fields = line.split("\t");

        //判断数据来源于那张表
        // order表三个字段，id pid amount
        if (filename.startsWith("order")) {
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));

            //占位，防止序列化时，顺序不一致
            tableBean.setPname("");
        } else {

            //pd表两个字段，pid pname
            tableBean.setPid(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setId("");
            tableBean.setAmount(0);
        }
        context.write(tableBean, NullWritable.get());
    }
}
