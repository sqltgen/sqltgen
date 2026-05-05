package com.example.db.models;

public record Sale(
    int id,
    int customerId,
    String orderedAt
) {}
