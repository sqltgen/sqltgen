package com.example.db.pg;

public record Sale(
    long id,
    long customerId,
    java.time.LocalDateTime orderedAt
) {}
