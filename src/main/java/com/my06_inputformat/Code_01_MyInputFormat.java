package com.my06_inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/24  9:53
 */

//自定义InputFormat，一次读取一个完整文件，并将其封装为k-v。key是文件全限定名，value是文件内容
public class Code_01_MyInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        Code_02_MyRecordReader recordReader = new Code_02_MyRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }
}
