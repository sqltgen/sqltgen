package com.example.db.models;

public record Book(
    long id,
    long authorId,
    String title,
    Genre genre,
    java.math.BigDecimal price,
    java.time.LocalDate publishedAt
) {}
