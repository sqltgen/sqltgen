package com.example.db.models;

public record SaleItem(
    int id,
    int saleId,
    int bookId,
    int quantity,
    double unitPrice
) {}
