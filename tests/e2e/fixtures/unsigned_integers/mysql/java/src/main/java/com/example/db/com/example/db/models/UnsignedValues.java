package com.example.db.models;

public record UnsignedValues(
    java.math.BigInteger id,
    short u8Val,
    int u16Val,
    long u24Val,
    long u32Val,
    java.math.BigInteger u64Val
) {}
