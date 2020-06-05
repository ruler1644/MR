package com.my11_groupcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther wu
 * @Date 2019/6/25  9:32
 */

//自定义Bean，实现排序规则

/**
 * （1）利用“订单id和成交金额”作为key，可以将Map阶段读取到的所有订单数据按照id升序排序，如果id相同再按照金额降序排序，发送到Reduce。
 * （2）在Reduce端利用groupingComparator将订单id相同的k-v聚合成组，然后取第一个即是该订单中最贵商品
 */
public class Code_01_OrderBean implements WritableComparable<Code_01_OrderBean> {

    private String OrderId;
    private String ProductId;
    private double Price;

    public Code_01_OrderBean() {

    }

    public Code_01_OrderBean(String OrderId, String productId, double Price) {
        this.OrderId = OrderId;
        this.ProductId = productId;
        this.Price = Price;
    }


    //定义比较规则，实现二次排序
    @Override
    public int compareTo(Code_01_OrderBean o) {

        //先按订单id，正序排序
        int res = OrderId.compareTo(o.OrderId);
        if (res == 0) {

            //再按价格，降序排序
            return Double.compare(o.Price, this.Price);
        } else {
            return res;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(OrderId);
        out.writeUTF(ProductId);
        out.writeDouble(Price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        OrderId = in.readUTF();
        ProductId = in.readUTF();
        Price = in.readDouble();
    }

    @Override
    public String toString() {
        return OrderId + "\t" + ProductId + "\t" + Price;
    }


    public String getOrderId() {
        return OrderId;
    }

    public void setOrderId(String orderId) {
        OrderId = orderId;
    }

    public String getProductId() {
        return ProductId;
    }

    public void setProductId(String productId) {
        ProductId = productId;
    }

    public double getPrice() {
        return Price;
    }

    public void setPrice(double price) {
        Price = price;
    }
}