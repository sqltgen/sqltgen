package com.example.db;

public record Author(
    int id,
    String name,
    String bio,
    Integer birthYear
) {}
