package org.example.backend.entity;

public class Indicators {
    private String names;
    private int max;

    public Indicators() {
    }

    public Indicators(String names, int max) {
        this.names = names;
        this.max = max;
    }

    @Override
    public String toString() {
        return "Indicators{" +
                "names='" + names + '\'' +
                ", max=" + max +
                '}';
    }

    public String getNames() {
        return names;
    }

    public void setNames(String names) {
        this.names = names;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }
}
