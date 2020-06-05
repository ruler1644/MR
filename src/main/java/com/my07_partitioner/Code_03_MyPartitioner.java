package com.my07_partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther wu
 * @Date 2019/6/27  9:29
 */

//泛型是接收的Map处理完的数据，key是手机号，value是FlowBean
public class Code_03_MyPartitioner extends Partitioner<Text, Code_01_FlowBean> {

    //对每一个K-V值，设置其分区号
    @Override
    public int getPartition(Text text, Code_01_FlowBean code_01_flowBean, int numPartitions) {

        //截取手机号前三位
        String pre = text.toString().substring(0, 3);
        switch (pre) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
