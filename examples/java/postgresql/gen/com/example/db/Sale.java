package com.example.db;

public record Sale(
    long id,
    long customerId,
    java.time.LocalDateTime orderedAt
) {}
