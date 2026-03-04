package com.example.db.sqlite;

public record Sale(
    int id,
    int customerId,
    Object orderedAt
) {}
