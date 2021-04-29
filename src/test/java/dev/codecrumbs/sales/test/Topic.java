package dev.codecrumbs.sales.test;

public enum Topic {
    ORDERS("orders"), SALES("sales");

    private String value;

    Topic(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
