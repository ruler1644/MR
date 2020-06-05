package com.my14_mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/6/30  18:08
 */

//MapJoin案例
public class Code_01_DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text output = new Text();

    //任务开始前，setup处理小表的数据，将其缓存到map端内存中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //分布式缓存可以有多个，是个数组
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //文件系统，据path开流，转换为缓冲字符流，一次读取一行
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream pdFilefis = fileSystem.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(pdFilefis, "utf-8"));

        //pid  pname
        //01 小米
        //按行处理数据
        String line = null;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {

            //切割数据
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(reader);
    }


    //Map方法只处理大表的数据
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //id      pid   amount
        //1001    01    1
        String[] fields = value.toString().split("\t");
        String pid = fields[1];

        //pid被pname替换的工作，在Map端完成
        output.set(fields[0] + "\t" + pdMap.get(pid) + "\t" + fields[2]);
        context.write(output, NullWritable.get());
    }
}
