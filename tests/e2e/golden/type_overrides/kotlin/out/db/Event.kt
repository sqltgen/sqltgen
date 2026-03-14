package db

import com.fasterxml.jackson.databind.JsonNode
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.UUID

data class Event(
    val id: Long,
    val name: String,
    val payload: JsonNode,
    val meta: JsonNode?,
    val docId: UUID,
    val createdAt: LocalDateTime,
    val scheduledAt: OffsetDateTime?,
    val eventDate: LocalDate?,
    val eventTime: LocalTime?
)
