package com.example.db.pg

data class Posts(
    val id: Long,
    val userId: Long,
    val title: String,
    val body: String?
)
