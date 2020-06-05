package com.my06_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/24  10:00
 */
public class Code_02_MyRecordReader extends RecordReader<Text, BytesWritable> {

    boolean isReader = false;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    //IO流
    FileSplit split1;
    FSDataInputStream fis;
    Configuration conf;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {


        //获取Configuration
        conf = context.getConfiguration();

        //通过切片，获取文件切片的路径
        split1 = (FileSplit) split;
        Path path = split1.getPath();

        //打开输入流
        //FileSystem fileSystem = path.getFileSystem(conf);
        FileSystem fileSystem = FileSystem.get(conf);
        fis = fileSystem.open(path);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        //如果文件没读过，读文件，封装成key-value
        if (!isReader) {

            //读取key
            String path = split1.getPath().toString();
            key.set(path);

            //读取value
            //获取切片长度
            long len = (long) split1.getLength();
            byte[] buff = new byte[(int) len];
            fis.read(buff);
            value.set(buff, 0, buff.length);

            //修改标记
            isReader = true;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    //一次读取一个完整文件，封装为k-v。key是文件全限定名，value是一个文件的内容
    //getProgress()要么是0，要么是1
    public float getProgress() throws IOException, InterruptedException {

        //如果读过返回1，否则返回0
        return (isReader ? 1 : 0);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(fis);
    }
}
