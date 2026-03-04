package com.example.db.pg;

public record Book(
    long id,
    long authorId,
    String title,
    String genre,
    java.math.BigDecimal price,
    java.time.LocalDate publishedAt
) {}
