package com.example.db

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/**
 * End-to-end runtime tests for type overrides: Jackson JSON on SQLite.
 *
 * SQLite type mapping notes:
 *  - id               → Int  (INTEGER PRIMARY KEY)
 *  - payload/meta     → String (TEXT; JSON serialised manually)
 *  - createdAt/scheduledAt → LocalDateTime (DATETIME stored as text string)
 *  - eventDate        → LocalDate?
 *  - eventTime        → LocalTime?
 *
 * Uses an in-memory SQLite database — no external services required.
 */
class RuntimeTest {

    private lateinit var conn: Connection
    private val mapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.autoCommit = true
        val ddl = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../../fixtures/type_overrides/sqlite/schema.sql"))
        conn.createStatement().use { s ->
            for (stmt in ddl.split(";")) {
                val t = stmt.trim()
                if (t.isNotEmpty()) s.execute(t)
            }
        }
    }

    @AfterEach
    fun tearDown() {
        conn.close()
    }

    private fun json(raw: String): JsonNode = mapper.readTree(raw)
    private fun jsonStr(node: JsonNode): String = mapper.writeValueAsString(node)

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    fun testInsertAndGetEvent() {
        val payload = json("""{"type":"click","x":10}""")
        val meta    = json("""{"source":"web"}""")

        Queries.insertEvent(conn, "login",
            jsonStr(payload), jsonStr(meta), "doc-001",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), LocalDateTime.of(2024, 6, 1, 14, 0, 0),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0))

        val ev = Queries.getEvent(conn, 1)!!
        assertEquals("login",   ev.name)
        assertEquals("doc-001", ev.docId)
        assertEquals(payload,   json(ev.payload))
        assertEquals(meta,      json(ev.meta!!))
        assertEquals(LocalDate.of(2024, 6, 1), ev.eventDate)
        assertEquals(LocalTime.of(9, 0, 0),    ev.eventTime)
    }

    @Test
    fun testGetEventNotFound() {
        assertNull(Queries.getEvent(conn, 999))
    }

    // ─── :many tests ──────────────────────────────────────────────────────────

    @Test
    fun testListEvents() {
        Queries.insertEvent(conn, "alpha", "{}", null, "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "beta",  "{}", null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "gamma", "{}", null, "doc-3", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        val events = Queries.listEvents(conn)
        assertEquals(3,       events.size)
        assertEquals("alpha", events[0].name)
        assertEquals("beta",  events[1].name)
        assertEquals("gamma", events[2].name)
    }

    @Test
    fun testGetEventsByDateRange() {
        Queries.insertEvent(conn, "early", "{}", null, "doc-1", LocalDateTime.of(2024, 1, 1, 10, 0, 0), null, null, null)
        Queries.insertEvent(conn, "mid",   "{}", null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "late",  "{}", null, "doc-3", LocalDateTime.of(2024, 12, 1, 15, 0, 0), null, null, null)

        val events = Queries.getEventsByDateRange(conn,
            LocalDateTime.of(2024, 1, 1, 0, 0, 0), LocalDateTime.of(2024, 7, 1, 0, 0, 0))

        assertEquals(2,       events.size)
        assertEquals("early", events[0].name)
        assertEquals("mid",   events[1].name)
    }

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    fun testUpdatePayload() {
        Queries.insertEvent(conn, "test", """{"v":1}""", """{"source":"web"}""", "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        Queries.updatePayload(conn, """{"v":2}""", null, 1)

        val ev = Queries.getEvent(conn, 1)!!
        assertEquals(json("""{"v":2}"""), json(ev.payload))
        assertNull(ev.meta)
    }

    @Test
    fun testUpdateEventDate() {
        Queries.insertEvent(conn, "dated", "{}", null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, LocalDate.of(2024, 1, 1), null)

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1)

        val ev = Queries.getEvent(conn, 1)!!
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate)
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    fun testInsertEventRows() {
        val n = Queries.insertEventRows(conn, "rowtest", "{}", null,
            "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        assertEquals(1L, n)
    }

    // ─── projection tests ─────────────────────────────────────────────────────

    @Test
    fun testFindByDate() {
        val target = LocalDate.of(2024, 6, 15)
        Queries.insertEvent(conn, "dated", "{}", null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, target, null)

        val row = Queries.findByDate(conn, target)!!
        assertEquals("dated", row.name)
    }

    @Test
    fun testFindByDocId() {
        Queries.insertEvent(conn, "doctest", "{}", null, "unique-doc-id",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        val row = Queries.findByDocId(conn, "unique-doc-id")!!
        assertEquals("doctest", row.name)
    }

    // ─── count tests ──────────────────────────────────────────────────────────

    @Test
    fun testCountEvents() {
        for (i in 1..3) {
            Queries.insertEvent(conn, "ev$i", "{}", null, "doc-$i",
                LocalDateTime.of(2024, 6, i, 0, 0, 0), null, null, null)
        }

        val row = Queries.countEvents(conn, LocalDateTime.of(2024, 1, 1, 0, 0, 0))!!
        assertEquals(3L, row.total)
    }
}
