package com.example.db.models

data class Product(
    val id: String,
    val sku: String,
    val name: String,
    val active: Int,
    val weightKg: Float?,
    val rating: Float?,
    val metadata: String?,
    val thumbnail: ByteArray?,
    val createdAt: String,
    val stockCount: Int
)
