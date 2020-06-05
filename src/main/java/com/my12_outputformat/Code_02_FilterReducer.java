package com.my12_outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/30  15:47
 */
public class Code_02_FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        //一行数据，拼接换行符
        String line = key.toString();
        line = line + "\r\n";
        k.set(line);

        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
