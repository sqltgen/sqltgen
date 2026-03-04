package com.example.db.pg;

public record Customer(
    long id,
    String name,
    String email
) {}
