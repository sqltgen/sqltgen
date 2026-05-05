package com.example.db.models

data class Record(
    val id: Long,
    val label: String,
    val timestamps: List<java.time.LocalDateTime>,
    val uuids: List<java.util.UUID>
)
