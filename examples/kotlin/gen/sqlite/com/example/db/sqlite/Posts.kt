package com.example.db.sqlite

data class Posts(
    val id: Int,
    val userId: Int,
    val title: String,
    val body: String?
)
