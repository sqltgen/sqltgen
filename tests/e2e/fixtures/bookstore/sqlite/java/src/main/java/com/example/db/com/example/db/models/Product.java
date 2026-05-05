package com.example.db.models;

public record Product(
    String id,
    String sku,
    String name,
    int active,
    Float weightKg,
    Float rating,
    String metadata,
    byte[] thumbnail,
    String createdAt,
    int stockCount
) {}
