package db.queries

import java.sql.Connection
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.UUID
import db.models.Event

object Queries {
    private val objectMapper = ObjectMapper()
    private fun parseJson(raw: String): com.fasterxml.jackson.databind.JsonNode = objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode::class.java)
    private fun toJson(value: com.fasterxml.jackson.databind.JsonNode?): String? = if (value == null) null else objectMapper.writeValueAsString(value)

    private val SQL_GET_EVENT = """
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        WHERE id = ?;
    """.trimIndent()
    fun getEvent(conn: Connection, id: Long): Event? {
        conn.prepareStatement(SQL_GET_EVENT).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Event(rs.getLong(1), rs.getString(2), parseJson(rs.getString(3)), rs.getString(4)?.let { parseJson(it)  }, rs.getObject(5, UUID::class.java), rs.getObject(6, LocalDateTime::class.java), rs.getObject(7, OffsetDateTime::class.java), rs.getObject(8, LocalDate::class.java), rs.getObject(9, LocalTime::class.java))
            }
        }
    }

    private val SQL_LIST_EVENTS = """
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        ORDER BY id;
    """.trimIndent()
    fun listEvents(conn: Connection): List<Event> {
        conn.prepareStatement(SQL_LIST_EVENTS).use { ps ->
            val rows = mutableListOf<Event>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Event(rs.getLong(1), rs.getString(2), parseJson(rs.getString(3)), rs.getString(4)?.let { parseJson(it)  }, rs.getObject(5, UUID::class.java), rs.getObject(6, LocalDateTime::class.java), rs.getObject(7, OffsetDateTime::class.java), rs.getObject(8, LocalDate::class.java), rs.getObject(9, LocalTime::class.java)))
            }
            return rows
        }
    }

    private val SQL_INSERT_EVENT = """
        INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """.trimIndent()
    fun insertEvent(conn: Connection, name: String, payload: JsonNode, meta: JsonNode?, docId: UUID, createdAt: LocalDateTime, scheduledAt: OffsetDateTime?, eventDate: LocalDate?, eventTime: LocalTime?): Unit {
        conn.prepareStatement(SQL_INSERT_EVENT).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, toJson(payload), java.sql.Types.OTHER)
            ps.setObject(3, toJson(meta), java.sql.Types.OTHER)
            ps.setObject(4, docId)
            ps.setObject(5, createdAt)
            ps.setObject(6, scheduledAt)
            ps.setObject(7, eventDate)
            ps.setObject(8, eventTime)
            ps.executeUpdate()
        }
    }

    private val SQL_UPDATE_PAYLOAD = """
        UPDATE event SET payload = ?, meta = ? WHERE id = ?;
    """.trimIndent()
    fun updatePayload(conn: Connection, payload: JsonNode, meta: JsonNode?, id: Long): Unit {
        conn.prepareStatement(SQL_UPDATE_PAYLOAD).use { ps ->
            ps.setObject(1, toJson(payload), java.sql.Types.OTHER)
            ps.setObject(2, toJson(meta), java.sql.Types.OTHER)
            ps.setLong(3, id)
            ps.executeUpdate()
        }
    }

    data class FindByDateRow(
        val id: Long,
        val name: String
    )

    private val SQL_FIND_BY_DATE = """
        SELECT id, name FROM event WHERE event_date = ?;
    """.trimIndent()
    fun findByDate(conn: Connection, eventDate: LocalDate?): FindByDateRow? {
        conn.prepareStatement(SQL_FIND_BY_DATE).use { ps ->
            ps.setObject(1, eventDate)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return FindByDateRow(rs.getLong(1), rs.getString(2))
            }
        }
    }

    data class FindByUuidRow(
        val id: Long,
        val name: String
    )

    private val SQL_FIND_BY_UUID = """
        SELECT id, name FROM event WHERE doc_id = ?;
    """.trimIndent()
    fun findByUuid(conn: Connection, docId: UUID): FindByUuidRow? {
        conn.prepareStatement(SQL_FIND_BY_UUID).use { ps ->
            ps.setObject(1, docId)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return FindByUuidRow(rs.getLong(1), rs.getString(2))
            }
        }
    }

    private val SQL_INSERT_EVENT_ROWS = """
        INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """.trimIndent()
    fun insertEventRows(conn: Connection, name: String, payload: JsonNode, meta: JsonNode?, docId: UUID, createdAt: LocalDateTime, scheduledAt: OffsetDateTime?, eventDate: LocalDate?, eventTime: LocalTime?): Long {
        conn.prepareStatement(SQL_INSERT_EVENT_ROWS).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, toJson(payload), java.sql.Types.OTHER)
            ps.setObject(3, toJson(meta), java.sql.Types.OTHER)
            ps.setObject(4, docId)
            ps.setObject(5, createdAt)
            ps.setObject(6, scheduledAt)
            ps.setObject(7, eventDate)
            ps.setObject(8, eventTime)
            return ps.executeUpdate().toLong()
        }
    }

    private val SQL_GET_EVENTS_BY_DATE_RANGE = """
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        WHERE created_at BETWEEN ? AND ?
        ORDER BY created_at;
    """.trimIndent()
    fun getEventsByDateRange(conn: Connection, createdAt: LocalDateTime, createdAt2: LocalDateTime): List<Event> {
        conn.prepareStatement(SQL_GET_EVENTS_BY_DATE_RANGE).use { ps ->
            ps.setObject(1, createdAt)
            ps.setObject(2, createdAt2)
            val rows = mutableListOf<Event>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Event(rs.getLong(1), rs.getString(2), parseJson(rs.getString(3)), rs.getString(4)?.let { parseJson(it)  }, rs.getObject(5, UUID::class.java), rs.getObject(6, LocalDateTime::class.java), rs.getObject(7, OffsetDateTime::class.java), rs.getObject(8, LocalDate::class.java), rs.getObject(9, LocalTime::class.java)))
            }
            return rows
        }
    }

    data class CountEventsRow(
        val total: Long
    )

    private val SQL_COUNT_EVENTS = """
        SELECT COUNT(*) AS total FROM event WHERE created_at > ?;
    """.trimIndent()
    fun countEvents(conn: Connection, createdAt: LocalDateTime): CountEventsRow? {
        conn.prepareStatement(SQL_COUNT_EVENTS).use { ps ->
            ps.setObject(1, createdAt)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return CountEventsRow(rs.getLong(1))
            }
        }
    }

    private val SQL_UPDATE_EVENT_DATE = """
        UPDATE event SET event_date = ? WHERE id = ?;
    """.trimIndent()
    fun updateEventDate(conn: Connection, eventDate: LocalDate?, id: Long): Unit {
        conn.prepareStatement(SQL_UPDATE_EVENT_DATE).use { ps ->
            ps.setObject(1, eventDate)
            ps.setLong(2, id)
            ps.executeUpdate()
        }
    }

    private fun getNullableBoolean(rs: java.sql.ResultSet, col: Int): Boolean? {
        val v = rs.getBoolean(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableShort(rs: java.sql.ResultSet, col: Int): Short? {
        val v = rs.getShort(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableInt(rs: java.sql.ResultSet, col: Int): Int? {
        val v = rs.getInt(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableLong(rs: java.sql.ResultSet, col: Int): Long? {
        val v = rs.getLong(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableFloat(rs: java.sql.ResultSet, col: Int): Float? {
        val v = rs.getFloat(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableDouble(rs: java.sql.ResultSet, col: Int): Double? {
        val v = rs.getDouble(col)
        return if (rs.wasNull()) null else v
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> jdbcArrayToList(arr: java.sql.Array): List<T> =
        (arr.array as Array<T>).toList()
}
