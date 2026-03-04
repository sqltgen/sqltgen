package com.example.db.pg

data class Author(
    val id: Long,
    val name: String,
    val bio: String?,
    val birthYear: Int?
)
