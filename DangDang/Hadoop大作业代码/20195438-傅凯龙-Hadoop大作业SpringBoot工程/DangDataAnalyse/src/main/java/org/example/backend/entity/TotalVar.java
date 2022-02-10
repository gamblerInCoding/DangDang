package org.example.backend.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TotalVar implements Writable {
    private int comment;
    private double price;
    public TotalVar() {
        super();
    }
    public TotalVar(int comment,double price) {
        super();
        this.comment = comment;
        this.price = price;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(comment);
        dataOutput.writeDouble(price);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.comment = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return "TotalVar{" +
                "comment=" + comment +
                ", price=" + price +
                '}';
    }

    public int getComment() {
        return comment;
    }

    public void setComment(int comment) {
        this.comment = comment;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
