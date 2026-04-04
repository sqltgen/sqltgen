package com.example.db.models

data class Sale(
    val id: Long,
    val customerId: Long,
    val orderedAt: java.time.LocalDateTime
)
