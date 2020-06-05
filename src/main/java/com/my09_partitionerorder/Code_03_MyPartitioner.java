package com.my09_partitionerorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther wu
 * @Date 2019/6/27  9:29
 */

//MyPartitioner接收的Map处理完的数据，key是FlowBean，value是手机号
public class Code_03_MyPartitioner extends Partitioner<Code_01_FlowBean, Text> {


    @Override
    public int getPartition(Code_01_FlowBean code_01_flowBean, Text text, int numPartitions) {

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
