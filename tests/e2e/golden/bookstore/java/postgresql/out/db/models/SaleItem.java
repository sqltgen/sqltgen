package db.models;

public record SaleItem(
    long id,
    long saleId,
    long bookId,
    int quantity,
    java.math.BigDecimal unitPrice
) {}
