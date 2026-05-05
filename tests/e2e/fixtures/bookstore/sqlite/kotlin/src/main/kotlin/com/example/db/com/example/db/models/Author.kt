package com.example.db.models

data class Author(
    val id: Int,
    val name: String,
    val bio: String?,
    val birthYear: Int?
)
