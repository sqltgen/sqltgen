package com.example.db.models;

public record Sale(
    long id,
    long customerId,
    java.time.LocalDateTime orderedAt
) {}
