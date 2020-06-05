package com.my09_partitionerorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  17:03
 */

//Map端输出数据，key是FlowBean，value是手机号
//Reducer端直接将key和value换位置即可，因为数据已经按key排好序了
public class Code_04_FlowReducer extends Reducer<Code_01_FlowBean, Text, Text, Code_01_FlowBean> {
    @Override
    protected void reduce(Code_01_FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //13736230513	4962	49362	54324
        //13736230512	4962	49362	54324
        for (Text val : values) {
            context.write(val, key);
        }
    }
}
