package com.example.db.pg

data class Sale(
    val id: Long,
    val customerId: Long,
    val orderedAt: java.time.LocalDateTime
)
