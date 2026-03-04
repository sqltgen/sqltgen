package com.example.db.sqlite

data class Author(
    val id: Int,
    val name: String,
    val bio: String?,
    val birthYear: Int?
)
