package org.example.backend.entity;

public class BookItems2 {
   private double commenttimes;
   private double price;

    public BookItems2() {
    }

    public BookItems2(double commenttimes, double price) {
        this.commenttimes = commenttimes;
        this.price = price;
    }

    @Override
    public String toString() {
        return "BookItems2{" +
                "commenttimes=" + commenttimes +
                ", price=" + price +
                '}';
    }

    public double getCommenttimes() {
        return commenttimes;
    }

    public void setCommenttimes(double commenttimes) {
        this.commenttimes = commenttimes;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
