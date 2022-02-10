package org.example.backend.entity;


public class HBasePoJo {
    private String family;
    private String column;

    public HBasePoJo(String family, String column) {
        this.family = family;
        this.column = column;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}
