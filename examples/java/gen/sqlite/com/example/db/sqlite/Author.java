package com.example.db.sqlite;

public record Author(
    int id,
    String name,
    String bio,
    Integer birthYear
) {}
