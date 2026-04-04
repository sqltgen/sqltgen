package com.example.db.models

import java.time.LocalDateTime
import java.util.UUID

data class Record(
    val id: Long,
    val label: String,
    val timestamps: List<LocalDateTime>,
    val uuids: List<UUID>
)
