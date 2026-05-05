package com.example.db.models;

public record Sale(
    int id,
    int customerId,
    java.time.LocalDateTime orderedAt
) {}
