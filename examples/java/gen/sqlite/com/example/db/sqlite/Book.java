package com.example.db.sqlite;

public record Book(
    int id,
    int authorId,
    String title,
    String genre,
    java.math.BigDecimal price,
    String publishedAt
) {}
