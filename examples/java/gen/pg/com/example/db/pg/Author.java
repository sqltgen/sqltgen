package com.example.db.pg;

public record Author(
    long id,
    String name,
    String bio,
    Integer birthYear
) {}
