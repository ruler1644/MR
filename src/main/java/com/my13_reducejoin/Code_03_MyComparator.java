package com.my13_reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author wu
 * @Date 2020/6/03
 */

//分组规则：按pid分组
public class Code_03_MyComparator extends WritableComparator {

    //序列化bean
    protected Code_03_MyComparator() {
        super(Code_01_TableBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Code_01_TableBean tableBean1 = (Code_01_TableBean) a;
        Code_01_TableBean tableBean2 = (Code_01_TableBean) b;

        return tableBean1.getPid().compareTo(tableBean2.getPid());
    }
}
