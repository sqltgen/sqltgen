package com.example.db.models

data class Book(
    val id: Int,
    val authorId: Int,
    val title: String,
    val genre: String,
    val price: Double,
    val publishedAt: String?
)
