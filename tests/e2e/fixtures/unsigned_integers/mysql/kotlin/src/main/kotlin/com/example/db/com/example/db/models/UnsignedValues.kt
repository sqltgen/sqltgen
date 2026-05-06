package com.example.db.models

data class UnsignedValues(
    val id: java.math.BigInteger,
    val u8Val: Short,
    val u16Val: Int,
    val u24Val: Long,
    val u32Val: Long,
    val u64Val: java.math.BigInteger
)
