package com.my11_groupcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  16:25
 */

//0000001	Pdt_01	222.8
//0000002	Pdt_05	722.4
//在key位置上的Bean，已经将信息存储完毕
//map阶段，Bean的排序规则是：先按照订单id升序排序，再按商品价格降序排序
public class Code_02_OrderMapper extends Mapper<LongWritable, Text, Code_01_OrderBean, NullWritable> {

    Code_01_OrderBean orderBean = new Code_01_OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        orderBean.setOrderId((fields[0]));
        orderBean.setProductId((fields[1]));
        orderBean.setPrice(Double.parseDouble(fields[2]));

        context.write(orderBean, NullWritable.get());
    }
}
