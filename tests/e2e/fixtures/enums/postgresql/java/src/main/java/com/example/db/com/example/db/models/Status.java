package com.example.db.models;

public enum Status {
    OPEN("open"),
    IN_PROGRESS("in_progress"),
    DONE("done"),
    CANCELLED("cancelled");

    private final String value;

    Status(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Status fromValue(String value) {
        for (Status e : values()) {
            if (e.value.equals(value)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Unknown Status: " + value);
    }
}
