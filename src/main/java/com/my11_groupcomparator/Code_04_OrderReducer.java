package com.my11_groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Auther wu
 * @Date 2019/6/25  17:03
 */
public class Code_04_OrderReducer extends Reducer<Code_01_OrderBean, NullWritable, Code_01_OrderBean, NullWritable> {

    //只要订单id相同，就认为是一个组，进入同一个Reducer
    //所以，有多少个订单，Reducer中就对应有多个key
    @Override
    protected void reduce(Code_01_OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        //1 每个订单最贵的商品
        //0000001	Pdt_01	222.8
        //0000002	Pdt_05	722.4
        //0000003	Pdt_06	232.8
        //context.write(key, NullWritable.get());


        //2 所有订单下的商品
        //0000001	Pdt_01	222.8
        //0000001	Pdt_02	33.8
        //0000002	Pdt_05	722.4
        //0000002	Pdt_03	522.8
        //0000002	Pdt_04	122.4
        //0000003	Pdt_06	232.8
        //0000003	Pdt_02	33.8

        //key对象是同一个，但是属性值不断变化。
        /*for (NullWritable value : values) {
            context.write(key, value);

        }*/

        //3 对2结果的说明
        //MR组件间传输的是序列化好的数据不是对象。先创建k-v两个空对象，反序列化填充属性值
        //对象不变，值一直在变化。分组在hasNext()方法中发生
       /* Iterator<NullWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            NullWritable next = iterator.next();
            context.write(key, next);
        }*/


        //4 取出每个订单中价格最高的2个商品(分组topK问题)
        //0000001	Pdt_01	222.8
        //0000001	Pdt_02	33.8
        //0000002	Pdt_05	722.4
        //0000002	Pdt_03	522.8
        //0000003	Pdt_06	232.8
        //0000003	Pdt_02	33.8
        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {

            //使用if，外层for循环控制个数
            //不能使用while，若使用while的话，会将一组key对应的所有value都获取
            if (iterator.hasNext()) {
                NullWritable next = iterator.next();
                context.write(key, next);
            }
        }
    }
}
