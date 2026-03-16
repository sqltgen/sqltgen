package com.example.db

data class SaleItem(
    val id: Int,
    val saleId: Int,
    val bookId: Int,
    val quantity: Int,
    val unitPrice: Double
)
