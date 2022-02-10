package org.example.backend.entity;

public class PulishItem {
    private String rowKey;
    private String publisher;
    private String number;

    public PulishItem(String rowKey, String publisher, String number) {
        this.rowKey = rowKey;
        this.publisher = publisher;
        this.number = number;
    }

    public PulishItem() {

    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
