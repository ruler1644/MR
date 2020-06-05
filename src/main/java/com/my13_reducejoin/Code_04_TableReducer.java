package com.my13_reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Auther wu
 * @Date 2019/6/30  18:08
 */

//pid相同的数据，会进入同一个Reducer
//01	小米
//1001	01	1
//1004	01	4

//排序规则：先按pid排序，相同时再按照pname排序
//分组规则：按pid分组
public class Code_04_TableReducer extends Reducer<Code_01_TableBean, NullWritable, Code_01_TableBean, NullWritable> {
    @Override
    protected void reduce(Code_01_TableBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();

        //第一条数据来自pd，之后全部来自order
        //获取第一条数据，获取pname
        iterator.next();
        String pname = key.getPname();

        //从第二行开始遍历，把pname设置到后边数据中并写出
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
