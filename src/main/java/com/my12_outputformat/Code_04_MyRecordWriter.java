package com.my12_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/30  16:01
 */

public class Code_04_MyRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream fos1;
    FSDataOutputStream fos2;

    public Code_04_MyRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            // MyRecordWriter继承RecordWriter，并创建两个文件的输出流，用于将数据输出到不同文件
            fos1 = fs.create(new Path("d:/delete/output/12/atguigu.log"));
            fos2 = fs.create(new Path("d:/delete/output/12/other.log"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value)
            throws IOException, InterruptedException {

        String str = key.toString();
        if (str.contains("atguigu")) {

            //含有"atguigu"，输出到文件atguigu.log
            fos1.write(str.getBytes());
        } else {
            fos2.write(str.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {
        IOUtils.closeStream(fos1);
        IOUtils.closeStream(fos2);
    }
}
