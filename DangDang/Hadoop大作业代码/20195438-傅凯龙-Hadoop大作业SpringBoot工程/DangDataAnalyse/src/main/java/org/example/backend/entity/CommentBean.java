package org.example.backend.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommentBean implements Writable {


    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getComment() {
        return comment;
    }

    public void setComment(int comment) {
        this.comment = comment;
    }

    private int comment;
    private double price;
//2 反序列化时，需要反射调用空参构造函数，所以必须有
    public CommentBean() {
        super();
    }
    public CommentBean(int upFlow, double downFlow) {
        super();
        this.comment = upFlow;
        this.price = downFlow;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(comment);
        dataOutput.writeDouble(price);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.comment = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return "CommentBean{" +
                "comment=" + comment +
                ", price=" + price +
                '}';
    }
}