package com.example.db;

public record Author(
    long id,
    String name,
    String bio,
    Integer birthYear
) {}
