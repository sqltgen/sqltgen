package com.example.db.pg;

public record SaleItem(
    long id,
    long saleId,
    long bookId,
    int quantity,
    java.math.BigDecimal unitPrice
) {}
