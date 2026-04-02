package com.example.db;

import java.time.LocalDateTime;
import java.util.UUID;

public record Record(
    long id,
    String label,
    java.util.List<LocalDateTime> timestamps,
    java.util.List<UUID> uuids
) {}
