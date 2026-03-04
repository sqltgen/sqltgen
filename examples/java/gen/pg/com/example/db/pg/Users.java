package com.example.db.pg;

public record Users(
    long id,
    String name,
    String email,
    String bio
) {}
