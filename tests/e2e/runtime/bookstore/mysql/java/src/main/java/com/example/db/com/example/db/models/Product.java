package com.example.db.models;

public record Product(
    String id,
    String sku,
    String name,
    boolean active,
    Float weightKg,
    Double rating,
    String metadata,
    byte[] thumbnail,
    java.time.LocalDateTime createdAt,
    short stockCount
) {}
