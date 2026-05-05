package com.example.db.models;

public record Book(
    int id,
    int authorId,
    String title,
    String genre,
    double price,
    String publishedAt
) {}
