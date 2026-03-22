package com.example.db;

public record Sale(
    int id,
    int customerId,
    java.time.LocalDateTime orderedAt
) {}
