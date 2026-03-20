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
import java.util.UUID

/**
 * End-to-end runtime tests for type overrides: Jackson JSON on MySQL.
 *
 * Each test creates a dedicated MySQL database named test_&lt;uuid&gt; to provide
 * full isolation. Requires the docker-compose MySQL service on port 13306.
 */
class RuntimeTest {

    private val rootUrl = System.getenv()
        .getOrDefault("MYSQL_ROOT_URL", "jdbc:mysql://localhost:13306/sqltgen_e2e")
    private val testBaseUrl = System.getenv()
        .getOrDefault("DATABASE_URL", "jdbc:mysql://localhost:13306/")

    private lateinit var conn: Connection
    private lateinit var dbName: String
    private val mapper = ObjectMapper()

    @BeforeEach
    fun setUp() {
        dbName = "test_" + UUID.randomUUID().toString().replace("-", "")
        DriverManager.getConnection(rootUrl, "root", "sqltgen").use { admin ->
            admin.createStatement().use { s ->
                s.execute("CREATE DATABASE `$dbName`")
                s.execute("GRANT ALL ON `$dbName`.* TO 'sqltgen'@'%'")
            }
        }
        conn = DriverManager.getConnection(
            "${testBaseUrl}${dbName}?useSSL=false&allowPublicKeyRetrieval=true",
            "sqltgen", "sqltgen"
        )
        conn.autoCommit = true
        val ddl = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../../fixtures/type_overrides/mysql/schema.sql"))
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
        DriverManager.getConnection(rootUrl, "root", "sqltgen").use { admin ->
            admin.createStatement().use { s ->
                s.execute("DROP DATABASE IF EXISTS `$dbName`")
            }
        }
    }

    private fun json(raw: String): JsonNode = mapper.readTree(raw)

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    fun testInsertAndGetEvent() {
        val payload = json("""{"type":"click","x":10}""")
        val meta = json("""{"source":"web"}""")

        Queries.insertEvent(conn, "login", payload, meta, "doc-001",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), LocalDateTime.of(2024, 6, 1, 14, 0, 0),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0))

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals("login", ev.name)
        assertEquals("doc-001", ev.docId)
        assertEquals(LocalDate.of(2024, 6, 1), ev.eventDate)
        assertEquals(LocalTime.of(9, 0, 0), ev.eventTime)
    }

    @Test
    fun testGetEventNotFound() {
        assertNull(Queries.getEvent(conn, 999L))
    }

    // ─── :many tests ──────────────────────────────────────────────────────────

    @Test
    fun testListEvents() {
        Queries.insertEvent(conn, "alpha", json("{}"), null, "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "beta",  json("{}"), null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "gamma", json("{}"), null, "doc-3", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        val events = Queries.listEvents(conn)
        assertEquals(3, events.size)
        assertEquals("alpha", events[0].name)
        assertEquals("beta",  events[1].name)
        assertEquals("gamma", events[2].name)
    }

    @Test
    fun testGetEventsByDateRange() {
        Queries.insertEvent(conn, "early", json("{}"), null, "doc-1", LocalDateTime.of(2024, 1, 1, 10, 0, 0), null, null, null)
        Queries.insertEvent(conn, "mid",   json("{}"), null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        Queries.insertEvent(conn, "late",  json("{}"), null, "doc-3", LocalDateTime.of(2024, 12, 1, 15, 0, 0), null, null, null)

        val events = Queries.getEventsByDateRange(conn,
            LocalDateTime.of(2024, 1, 1, 0, 0, 0), LocalDateTime.of(2024, 7, 1, 0, 0, 0))

        assertEquals(2, events.size)
        assertEquals("early", events[0].name)
        assertEquals("mid",   events[1].name)
    }

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    fun testUpdatePayload() {
        Queries.insertEvent(conn, "test", json("""{"v":1}"""), null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        val updated = json("""{"v":2,"changed":true}""")
        Queries.updatePayload(conn, updated, null, 1L)

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals(updated, ev.payload)
        assertNull(ev.meta)
    }

    @Test
    fun testUpdateEventDate() {
        Queries.insertEvent(conn, "dated", json("{}"), null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, LocalDate.of(2024, 1, 1), null)

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1L)

        val ev = Queries.getEvent(conn, 1L)!!
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate)
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    fun testInsertEventRows() {
        val n = Queries.insertEventRows(conn, "rowtest", json("{}"), null,
            "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)
        assertEquals(1L, n)
    }

    // ─── projection tests ─────────────────────────────────────────────────────

    @Test
    fun testFindByDate() {
        Queries.insertEvent(conn, "dated", json("{}"), null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, LocalDate.of(2024, 6, 15), null)

        val row = Queries.findByDate(conn, LocalDate.of(2024, 6, 15))!!
        assertEquals("dated", row.name)
    }

    @Test
    fun testFindByDocId() {
        Queries.insertEvent(conn, "doctest", json("{}"), null, "unique-doc-id",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null)

        val row = Queries.findByDocId(conn, "unique-doc-id")!!
        assertEquals("doctest", row.name)
    }

    // ─── count tests ──────────────────────────────────────────────────────────

    @Test
    fun testCountEvents() {
        for (i in 1..3) {
            Queries.insertEvent(conn, "ev$i", json("{}"), null, "doc-$i",
                LocalDateTime.of(2024, 6, i, 0, 0, 0), null, null, null)
        }

        val row = Queries.countEvents(conn, LocalDateTime.of(2024, 1, 1, 0, 0, 0))!!
        assertEquals(3L, row.total)
    }
}
