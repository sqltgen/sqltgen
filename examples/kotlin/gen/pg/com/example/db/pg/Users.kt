package com.example.db.pg

data class Users(
    val id: Long,
    val name: String,
    val email: String,
    val bio: String?
)
