package org.example.backend.entity;

public class FlowBeans {
    private String raking;
    private String name;
    private double price;
    private int comment;
    public FlowBeans(String raking, String name, double price,int comment) {
        this.raking = raking;
        this.name = name;
        this.price = price;
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "FlowBeans{" +
                "raking='" + raking + '\'' +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", comment=" + comment +
                '}';
    }

    public int getComment() {
        return comment;
    }

    public void setComment(int comment) {
        this.comment = comment;
    }

    public String getRaking() {
        return raking;
    }

    public void setRaking(String raking) {
        this.raking = raking;
    }

    public String getName() {
        return name;
    }

    public FlowBeans() {
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
