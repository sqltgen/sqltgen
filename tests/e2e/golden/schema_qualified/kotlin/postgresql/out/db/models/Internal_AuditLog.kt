package db.models

data class Internal_AuditLog(
    val id: Long,
    val userId: Long,
    val action: String,
    val createdAt: java.time.LocalDateTime
)
