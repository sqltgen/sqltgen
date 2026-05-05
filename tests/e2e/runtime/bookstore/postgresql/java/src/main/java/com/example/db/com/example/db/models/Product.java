package com.example.db.models;

public record Product(
    java.util.UUID id,
    String sku,
    String name,
    boolean active,
    Float weightKg,
    Double rating,
    java.util.List<String> tags,
    String metadata,
    byte[] thumbnail,
    java.time.LocalDateTime createdAt,
    short stockCount
) {}
