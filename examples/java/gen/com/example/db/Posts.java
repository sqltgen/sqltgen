package com.example.db;

public record Posts(
    long id,
    long userId,
    String title,
    String body
) {}
