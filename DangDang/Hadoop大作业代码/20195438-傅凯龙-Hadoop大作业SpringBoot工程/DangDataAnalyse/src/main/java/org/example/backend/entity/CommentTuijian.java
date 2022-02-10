package org.example.backend.entity;

public class CommentTuijian {
    private double comment;
    private double Tuijian;

    public CommentTuijian(double comment, double tuijian) {
        this.comment = comment;
        Tuijian = tuijian;
    }

    public CommentTuijian() {
    }

    public double getComment() {
        return comment;
    }

    public void setComment(double comment) {
        this.comment = comment;
    }

    public double getTuijian() {
        return Tuijian;
    }

    public void setTuijian(double tuijian) {
        Tuijian = tuijian;
    }

    @Override
    public String toString() {
        return "CommentTuijian{" +
                "comment=" + comment +
                ", Tuijian=" + Tuijian +
                '}';
    }
}
