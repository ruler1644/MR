package com.my06_inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/26  19:58
 */
public class Code_04_SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {

        //遍历values，将所有的小文件写到一个文件中
        for (BytesWritable value : values) {
            context.write(key, value);
        }
    }
}
