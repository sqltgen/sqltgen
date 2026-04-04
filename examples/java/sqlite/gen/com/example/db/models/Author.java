package com.example.db.models;

public record Author(
    int id,
    String name,
    String bio,
    Integer birthYear
) {}
