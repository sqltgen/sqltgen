package com.example.db

import com.google.gson.JsonElement
import com.google.gson.JsonParser
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * End-to-end runtime tests for type overrides: Gson JSON + java.time on PostgreSQL.
 *
 * Each test runs in its own PostgreSQL schema to allow parallel execution.
 * Requires the docker-compose postgres service on port 15432.
 */
class RuntimeTest {

    private val url = System.getenv()
        .getOrDefault("DATABASE_URL", "jdbc:postgresql://localhost:15432/sqltgen_e2e")

    private lateinit var conn: Connection
    private lateinit var schema: String

    @BeforeEach
    fun setUp() {
        conn = DriverManager.getConnection(url, "sqltgen", "sqltgen")
        conn.autoCommit = true
        schema = "test_" + UUID.randomUUID().toString().replace("-", "")
        val ddl = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../../fixtures/type_overrides/postgresql/schema.sql"))
        conn.createStatement().use { s ->
            s.execute("""CREATE SCHEMA "$schema"""")
            s.execute("""SET search_path TO "$schema"""")
            s.execute(ddl)
        }
    }

    @AfterEach
    fun tearDown() {
        conn.createStatement().use { s ->
            s.execute("""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
        }
        conn.close()
    }

    private fun json(raw: String): JsonElement = JsonParser.parseString(raw)

    private fun sampleCreatedAt() = LocalDateTime.of(2024, 6, 1, 12, 0, 0)
    private fun sampleScheduledAt() = OffsetDateTime.of(2024, 6, 1, 14, 0, 0, 0, ZoneOffset.UTC)

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    fun testInsertAndGetEvent() {
        val docId = UUID.randomUUID()
        val payload = json("""{"type":"click","x":10}""")
        val meta = json("""{"source":"web"}""")

        Queries.insertEvent(conn, "login", payload, meta, docId,
            sampleCreatedAt(), sampleScheduledAt(),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0))

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals("login", ev.name)
        assertEquals(payload, ev.payload)
        assertEquals(meta, ev.meta)
        assertEquals(docId, ev.docId)
        assertEquals(sampleCreatedAt(), ev.createdAt)
        assertEquals(LocalDate.of(2024, 6, 1), ev.eventDate)
        assertEquals(LocalTime.of(9, 0, 0), ev.eventTime)
        assertEquals(sampleScheduledAt(), ev.scheduledAt)
    }

    @Test
    fun testGetEventNotFound() {
        assertNull(Queries.getEvent(conn, 999L))
    }

    // ─── :many tests ──────────────────────────────────────────────────────────

    @Test
    fun testListEvents() {
        Queries.insertEvent(conn, "alpha", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, null, null)
        Queries.insertEvent(conn, "beta", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, null, null)
        Queries.insertEvent(conn, "gamma", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, null, null)

        val events = Queries.listEvents(conn)
        assertEquals(3, events.size)
        assertEquals("alpha", events[0].name)
        assertEquals("beta", events[1].name)
        assertEquals("gamma", events[2].name)
    }

    @Test
    fun testGetEventsByDateRange() {
        val t1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0)
        val t2 = LocalDateTime.of(2024, 6, 1, 12, 0, 0)
        val t3 = LocalDateTime.of(2024, 12, 1, 15, 0, 0)

        Queries.insertEvent(conn, "early", json("{}"), null, UUID.randomUUID(), t1, null, null, null)
        Queries.insertEvent(conn, "mid",   json("{}"), null, UUID.randomUUID(), t2, null, null, null)
        Queries.insertEvent(conn, "late",  json("{}"), null, UUID.randomUUID(), t3, null, null, null)

        val events = Queries.getEventsByDateRange(conn,
            LocalDateTime.of(2024, 1, 1, 0, 0, 0),
            LocalDateTime.of(2024, 7, 1, 0, 0, 0))

        assertEquals(2, events.size)
        assertEquals("early", events[0].name)
        assertEquals("mid",   events[1].name)
    }

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    fun testUpdatePayload() {
        val originalMeta = json("""{"source":"web"}""")
        Queries.insertEvent(conn, "test", json("""{"v":1}"""), originalMeta, UUID.randomUUID(),
            sampleCreatedAt(), null, null, null)

        val updated = json("""{"v":2,"changed":true}""")
        Queries.updatePayload(conn, updated, null, 1L)

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals(updated, ev.payload)
        assertNull(ev.meta)
    }

    @Test
    fun testUpdateEventDate() {
        Queries.insertEvent(conn, "dated", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, LocalDate.of(2024, 1, 1), null)

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1L)

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate)
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    fun testInsertEventRows() {
        val n = Queries.insertEventRows(conn, "rowtest", json("{}"), null,
            UUID.randomUUID(), sampleCreatedAt(), null, null, null)
        assertEquals(1L, n)
    }

    // ─── projection tests ─────────────────────────────────────────────────────

    @Test
    fun testFindByDate() {
        val target = LocalDate.of(2024, 6, 15)
        Queries.insertEvent(conn, "dated", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, target, null)

        val row = Queries.findByDate(conn, target)!!
        assertEquals("dated", row.name)
    }

    @Test
    fun testFindByUuid() {
        val docId = UUID.randomUUID()
        Queries.insertEvent(conn, "uuid-test", json("{}"), null, docId,
            sampleCreatedAt(), null, null, null)

        val row = Queries.findByUuid(conn, docId)!!
        assertEquals("uuid-test", row.name)
    }

    // ─── count tests ──────────────────────────────────────────────────────────

    @Test
    fun testCountEvents() {
        val cutoff = LocalDateTime.of(2024, 1, 1, 0, 0, 0)
        for (i in 1..3) {
            val ts = LocalDateTime.of(2024, 6, i, 0, 0, 0)
            Queries.insertEvent(conn, "ev$i", json("{}"), null, UUID.randomUUID(),
                ts, null, null, null)
        }

        val row = Queries.countEvents(conn, cutoff)!!
        assertEquals(3L, row.total)
    }
}
