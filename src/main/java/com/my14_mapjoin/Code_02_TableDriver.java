package com.my14_mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Auther wu
 * @Date 2019/6/30  18:08
 */
public class Code_02_TableDriver {
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Code_02_TableDriver.class);
        job.setMapperClass(Code_01_DistributedCacheMapper.class);

        //MapJoin不需要reduce，
        //job.setReducerClass(xxx);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //输入文件只有一个大的，小表加载到分布式缓存
        FileInputFormat.setInputPaths(job, new Path("d:/delete/input/14/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/delete/output/14"));

        //加载到分布式缓存，Reduce个数是0，即没有Reduce
        job.addCacheFile(new URI("file:///d:/delete/input/14/pd.txt"));
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
