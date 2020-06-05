package com.my17_compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @Auther wu
 * @Date 2019/7/1  21:09
 */

//测试压缩与解压缩
public class Code_01_Compression {
    public static void main(String[] args) throws Exception {

        compress("D:/delete/input/17/1.txt", BZip2Codec.class);
        decompress("D:/delete/input/17/1.txt.bz2");
    }


    //压缩的方法
    private static void compress(String path, Class<? extends CompressionCodec> codecClass) throws Exception {

        //反射方式创建对象
        Configuration configuration = new Configuration();
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, configuration);

        //开流
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fis = fileSystem.open(new Path(path));

        //拼接扩展名
        FSDataOutputStream fos = fileSystem.create(new Path(path + codec.getDefaultExtension()));

        //输出流包装压缩
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //流的对拷
        IOUtils.copyBytes(fis, cos, 1024 * 1024, false);

        //关闭流
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


    //解压缩的方法
    private static void decompress(String path) throws Exception {

        //获取文件压缩方式
        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(path));

        //开流
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fis = fileSystem.open(new Path(path));

        //输入流包装压缩
        CompressionInputStream cis = codec.createInputStream(fis);

        //移除扩展名
        FSDataOutputStream fos = fileSystem.create(new Path(
                path.substring(0, path.length() - codec.getDefaultExtension().length())));

        //流的对拷
        IOUtils.copyBytes(cis, fos, 1024 * 1024, false);

        //关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
        System.out.println("over");
    }
}
