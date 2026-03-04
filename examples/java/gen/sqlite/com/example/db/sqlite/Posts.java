package com.example.db.sqlite;

public record Posts(
    int id,
    int userId,
    String title,
    String body
) {}
