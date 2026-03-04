package com.example.db.pg;

public record Posts(
    long id,
    long userId,
    String title,
    String body
) {}
