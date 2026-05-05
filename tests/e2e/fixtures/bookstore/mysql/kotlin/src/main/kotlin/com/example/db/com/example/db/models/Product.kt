package com.example.db.models

data class Product(
    val id: String,
    val sku: String,
    val name: String,
    val active: Boolean,
    val weightKg: Float?,
    val rating: Double?,
    val metadata: String?,
    val thumbnail: ByteArray?,
    val createdAt: java.time.LocalDateTime,
    val stockCount: Short
)
