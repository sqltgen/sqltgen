package com.example.db;

public record Book(
    int id,
    int authorId,
    String title,
    String genre,
    double price,
    String publishedAt
) {}
