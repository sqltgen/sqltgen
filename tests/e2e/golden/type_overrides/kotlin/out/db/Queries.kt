package db

import java.sql.Connection
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.UUID

object Queries {
    private val objectMapper = ObjectMapper()

    private const val SQL_GET_EVENT = "SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time FROM event WHERE id = ?;"
    fun getEvent(conn: Connection, id: Long): Event? {
        conn.prepareStatement(SQL_GET_EVENT).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Event(rs.getLong(1), rs.getString(2), objectMapper.readValue(rs.getString(3), JsonNode::class.java), objectMapper.readValue(rs.getString(4), JsonNode::class.java), rs.getObject(5, UUID::class.java), rs.getObject(6, LocalDateTime::class.java), rs.getObject(7, OffsetDateTime::class.java), rs.getObject(8, LocalDate::class.java), rs.getObject(9, LocalTime::class.java))
            }
        }
    }

    private const val SQL_LIST_EVENTS = "SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time FROM event ORDER BY id;"
    fun listEvents(conn: Connection): List<Event> {
        conn.prepareStatement(SQL_LIST_EVENTS).use { ps ->
            val rows = mutableListOf<Event>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Event(rs.getLong(1), rs.getString(2), objectMapper.readValue(rs.getString(3), JsonNode::class.java), objectMapper.readValue(rs.getString(4), JsonNode::class.java), rs.getObject(5, UUID::class.java), rs.getObject(6, LocalDateTime::class.java), rs.getObject(7, OffsetDateTime::class.java), rs.getObject(8, LocalDate::class.java), rs.getObject(9, LocalTime::class.java)))
            }
            return rows
        }
    }

    private const val SQL_INSERT_EVENT = "INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
    fun insertEvent(conn: Connection, name: String, payload: JsonNode, meta: JsonNode?, docId: UUID, createdAt: LocalDateTime, scheduledAt: OffsetDateTime?, eventDate: LocalDate?, eventTime: LocalTime?): Unit {
        conn.prepareStatement(SQL_INSERT_EVENT).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, objectMapper.writeValueAsString(payload), java.sql.Types.OTHER)
            ps.setObject(3, objectMapper.writeValueAsString(meta), java.sql.Types.OTHER)
            ps.setObject(4, docId)
            ps.setObject(5, createdAt)
            ps.setObject(6, scheduledAt)
            ps.setObject(7, eventDate)
            ps.setObject(8, eventTime)
            ps.executeUpdate()
        }
    }

    private const val SQL_UPDATE_PAYLOAD = "UPDATE event SET payload = ?, meta = ? WHERE id = ?;"
    fun updatePayload(conn: Connection, payload: JsonNode, meta: JsonNode?, id: Long): Unit {
        conn.prepareStatement(SQL_UPDATE_PAYLOAD).use { ps ->
            ps.setObject(1, objectMapper.writeValueAsString(payload), java.sql.Types.OTHER)
            ps.setObject(2, objectMapper.writeValueAsString(meta), java.sql.Types.OTHER)
            ps.setLong(3, id)
            ps.executeUpdate()
        }
    }

    data class FindByDateRow(
        val id: Long,
        val name: String
    )

    private const val SQL_FIND_BY_DATE = "SELECT id, name FROM event WHERE event_date = ?;"
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

    private const val SQL_FIND_BY_UUID = "SELECT id, name FROM event WHERE doc_id = ?;"
    fun findByUuid(conn: Connection, docId: UUID): FindByUuidRow? {
        conn.prepareStatement(SQL_FIND_BY_UUID).use { ps ->
            ps.setObject(1, docId)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return FindByUuidRow(rs.getLong(1), rs.getString(2))
            }
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
}
