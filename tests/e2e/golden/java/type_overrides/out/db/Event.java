package db;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Event(
    long id,
    String name,
    JsonNode payload,
    JsonNode meta,
    UUID docId,
    LocalDateTime createdAt,
    OffsetDateTime scheduledAt,
    LocalDate eventDate,
    LocalTime eventTime
) {}
