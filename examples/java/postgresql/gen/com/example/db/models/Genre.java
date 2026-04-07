package com.example.db.models;

public enum Genre {
    FICTION("fiction"),
    NON_FICTION("non_fiction"),
    SCIENCE("science"),
    HISTORY("history"),
    BIOGRAPHY("biography");

    private final String value;

    Genre(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Genre fromValue(String value) {
        for (Genre e : values()) {
            if (e.value.equals(value)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Unknown Genre: " + value);
    }
}
