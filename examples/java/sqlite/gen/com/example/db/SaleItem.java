package com.example.db;

public record SaleItem(
    int id,
    int saleId,
    int bookId,
    int quantity,
    double unitPrice
) {}
