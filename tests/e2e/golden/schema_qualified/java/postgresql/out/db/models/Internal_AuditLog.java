package db.models;

public record Internal_AuditLog(
    long id,
    long userId,
    String action,
    java.time.LocalDateTime createdAt
) {}
