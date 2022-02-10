package org.example.backend.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    public double getTuijian() {
        return tuijian;
    }

    public void setTuijian(double tuijian) {
        this.tuijian = tuijian;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "tuijian=" + tuijian +
                ", price=" + price +
                '}';
    }

    private double tuijian;
    private double price;
//2 反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }
    public FlowBean(double upFlow, double downFlow) {
        super();
        this.tuijian = upFlow;
        this.price = downFlow;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(tuijian);
        dataOutput.writeDouble(price);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.tuijian = in.readDouble();
        this.price = in.readDouble();
    }

}