package com.example.db.sqlite

data class SaleItem(
    val id: Int,
    val saleId: Int,
    val bookId: Int,
    val quantity: Int,
    val unitPrice: java.math.BigDecimal
)
