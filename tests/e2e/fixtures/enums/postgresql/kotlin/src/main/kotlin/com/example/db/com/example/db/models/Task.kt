package com.example.db.models

data class Task(
    val id: Long,
    val title: String,
    val priority: Priority,
    val status: Status,
    val description: String?,
    val tags: List<Priority>
)
