package org.example.backend.entity;

public class BookItem {

    private String rowKey;
    private String ranking;
    private String title;
    private String author;
    private String prices;
    private String comment;
    private String recommend;
    private String publisher;

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRanking(String ranking) {
        this.ranking = ranking;
    }
    public String getRanking() {
        return this.ranking;
    }

    public java.lang.String getTitle() {
        return title;
    }

    public void setTitle(java.lang.String title) {
        this.title = title;
    }

    public java.lang.String getAuthor() {
        return author;
    }

    public void setAuthor(java.lang.String author) {
        this.author = author;
    }

    public java.lang.String getPrices() {
        return prices;
    }

    public void setPrices(java.lang.String prices) {
        this.prices = prices;
    }

    public java.lang.String getComment() {
        return comment;
    }

    public void setComment(java.lang.String comment) {
        this.comment = comment;
    }

    public java.lang.String getRecommend() {
        return recommend;
    }

    public void setRecommend(java.lang.String recommend) {
        this.recommend = recommend;
    }

    public BookItem(String rowKey, String ranking, String title, String author, String prices, String comment, String recommend, String publisher) {
        this.rowKey = rowKey;
        this.ranking = ranking;
        this.title = title;
        this.author = author;
        this.prices = prices;
        this.comment = comment;
        this.recommend = recommend;
        this.publisher = publisher;
    }

    @Override
    public String toString() {
        return "BookItem{" +
                "rowKey='" + rowKey + '\'' +
                ", ranking='" + ranking + '\'' +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", prices='" + prices + '\'' +
                ", comment='" + comment + '\'' +
                ", recommend='" + recommend + '\'' +
                ", publisher='" + publisher + '\'' +
                '}';
    }

    public BookItem() {

    }


}
