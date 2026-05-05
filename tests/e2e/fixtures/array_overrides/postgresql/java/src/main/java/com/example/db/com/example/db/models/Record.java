package com.example.db.models;

public record Record(
    long id,
    String label,
    java.util.List<java.time.LocalDateTime> timestamps,
    java.util.List<java.util.UUID> uuids
) {}
