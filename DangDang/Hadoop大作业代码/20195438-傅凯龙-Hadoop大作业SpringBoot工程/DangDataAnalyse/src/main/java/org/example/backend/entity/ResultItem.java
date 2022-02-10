package org.example.backend.entity;

public class ResultItem {
    private String rowKey;
    private String ranking;
    private String title;

    @java.lang.Override
    public String toString() {
        return "ResultItem{" +
                "rowKey=" + rowKey +
                ", ranking=" + ranking +
                ", title=" + title +
                '}';
    }

    public ResultItem(java.lang.String rowKey, java.lang.String ranking, java.lang.String title) {
        this.rowKey = rowKey;
        this.ranking = ranking;
        this.title = title;
    }
    public ResultItem() {

    }
    public java.lang.String getRowKey() {
        return rowKey;
    }

    public void setRowKey(java.lang.String rowKey) {
        this.rowKey = rowKey;
    }

    public java.lang.String getRanking() {
        return ranking;
    }

    public void setRanking(java.lang.String ranking) {
        this.ranking = ranking;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(java.lang.String title) {
        this.title = title;
    }
}
