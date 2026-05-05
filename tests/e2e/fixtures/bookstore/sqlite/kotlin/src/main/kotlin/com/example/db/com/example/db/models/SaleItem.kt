package com.example.db.models

data class SaleItem(
    val id: Int,
    val saleId: Int,
    val bookId: Int,
    val quantity: Int,
    val unitPrice: Double
)
