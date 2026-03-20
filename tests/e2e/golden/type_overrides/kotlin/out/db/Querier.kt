package db

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.UUID
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {
    private val objectMapper = ObjectMapper()
    private fun parseJson(raw: String): com.fasterxml.jackson.databind.JsonNode = objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode::class.java)
    private fun toJson(value: com.fasterxml.jackson.databind.JsonNode?): String? = if (value == null) null else objectMapper.writeValueAsString(value)

    fun getEvent(id: Long): Event? =
        dataSource.connection.use { conn -> Queries.getEvent(conn, id) }

    fun listEvents(): List<Event> =
        dataSource.connection.use { conn -> Queries.listEvents(conn) }

    fun insertEvent(name: String, payload: JsonNode, meta: JsonNode?, docId: UUID, createdAt: LocalDateTime, scheduledAt: OffsetDateTime?, eventDate: LocalDate?, eventTime: LocalTime?): Unit =
        dataSource.connection.use { conn -> Queries.insertEvent(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime) }

    fun updatePayload(payload: JsonNode, meta: JsonNode?, id: Long): Unit =
        dataSource.connection.use { conn -> Queries.updatePayload(conn, payload, meta, id) }

    fun findByDate(eventDate: LocalDate?): Queries.FindByDateRow? =
        dataSource.connection.use { conn -> Queries.findByDate(conn, eventDate) }

    fun findByUuid(docId: UUID): Queries.FindByUuidRow? =
        dataSource.connection.use { conn -> Queries.findByUuid(conn, docId) }

    fun insertEventRows(name: String, payload: JsonNode, meta: JsonNode?, docId: UUID, createdAt: LocalDateTime, scheduledAt: OffsetDateTime?, eventDate: LocalDate?, eventTime: LocalTime?): Long =
        dataSource.connection.use { conn -> Queries.insertEventRows(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime) }

    fun getEventsByDateRange(createdAt: LocalDateTime, createdAt2: LocalDateTime): List<Event> =
        dataSource.connection.use { conn -> Queries.getEventsByDateRange(conn, createdAt, createdAt2) }

    fun countEvents(createdAt: LocalDateTime): Queries.CountEventsRow? =
        dataSource.connection.use { conn -> Queries.countEvents(conn, createdAt) }

    fun updateEventDate(eventDate: LocalDate?, id: Long): Unit =
        dataSource.connection.use { conn -> Queries.updateEventDate(conn, eventDate, id) }
}
