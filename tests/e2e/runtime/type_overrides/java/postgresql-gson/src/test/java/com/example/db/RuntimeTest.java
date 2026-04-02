package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for type overrides: Gson JSON + java.time on PostgreSQL.
 *
 * Each test creates an isolated PostgreSQL schema so tests can run in parallel.
 * Requires the docker-compose postgres service on port 15432.
 */
class RuntimeTest {

    private static final String URL =
        System.getenv().getOrDefault("DATABASE_URL",
            "jdbc:postgresql://localhost:15432/sqltgen_e2e");
    private static final String USER = "sqltgen";
    private static final String PASS = "sqltgen";

    private Connection conn;
    private String schema;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        conn = DriverManager.getConnection(URL, USER, PASS);
        conn.setAutoCommit(true);
        schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        var ddl = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../../fixtures/type_overrides/postgresql/schema.sql"));
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA \"" + schema + "\"");
            s.execute("SET search_path TO \"" + schema + "\"");
            s.execute(ddl);
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
            conn.close();
        }
    }

    private JsonElement json(String raw) {
        return JsonParser.parseString(raw);
    }

    private LocalDateTime sampleCreatedAt() {
        return LocalDateTime.of(2024, 6, 1, 12, 0, 0);
    }

    private OffsetDateTime sampleScheduledAt() {
        return OffsetDateTime.of(2024, 6, 1, 14, 0, 0, 0, ZoneOffset.UTC);
    }

    // ─── :one tests ────────────────────────────────────────────────────────────

    @Test
    void testInsertAndGetEvent() throws Exception {

        var docId = UUID.randomUUID();
        var payload = json("{\"type\":\"click\",\"x\":10}");
        var meta = json("{\"source\":\"web\"}");

        Queries.insertEvent(conn, "login", payload, meta, docId,
            sampleCreatedAt(), sampleScheduledAt(),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0));

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals("login", ev.name());
        assertEquals(payload, ev.payload());
        assertEquals(meta, ev.meta());
        assertEquals(docId, ev.docId());
        assertEquals(sampleCreatedAt(), ev.createdAt());
        assertEquals(sampleScheduledAt(), ev.scheduledAt());
        assertEquals(LocalDate.of(2024, 6, 1), ev.eventDate());
        assertEquals(LocalTime.of(9, 0, 0), ev.eventTime());
    }

    @Test
    void testGetEventNotFound() throws Exception {
        assertTrue(Queries.getEvent(conn, 999L).isEmpty());
    }

    // ─── :many tests ───────────────────────────────────────────────────────────

    @Test
    void testListEvents() throws Exception {

        var docId1 = UUID.randomUUID();
        var docId2 = UUID.randomUUID();
        var docId3 = UUID.randomUUID();

        Queries.insertEvent(conn, "alpha", json("{}"), null, docId1,
            sampleCreatedAt(), null, null, null);
        Queries.insertEvent(conn, "beta", json("{}"), null, docId2,
            sampleCreatedAt(), null, null, null);
        Queries.insertEvent(conn, "gamma", json("{}"), null, docId3,
            sampleCreatedAt(), null, null, null);

        var events = Queries.listEvents(conn);
        assertEquals(3, events.size());
        assertEquals("alpha", events.get(0).name());
        assertEquals("beta", events.get(1).name());
        assertEquals("gamma", events.get(2).name());
    }

    @Test
    void testGetEventsByDateRange() throws Exception {

        var t1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        var t2 = LocalDateTime.of(2024, 6, 1, 12, 0, 0);
        var t3 = LocalDateTime.of(2024, 12, 1, 15, 0, 0);

        Queries.insertEvent(conn, "early", json("{}"), null, UUID.randomUUID(),
            t1, null, null, null);
        Queries.insertEvent(conn, "mid", json("{}"), null, UUID.randomUUID(),
            t2, null, null, null);
        Queries.insertEvent(conn, "late", json("{}"), null, UUID.randomUUID(),
            t3, null, null, null);

        var rangeStart = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        var rangeEnd   = LocalDateTime.of(2024, 7, 1, 0, 0, 0);
        var events = Queries.getEventsByDateRange(conn, rangeStart, rangeEnd);

        assertEquals(2, events.size());
        assertEquals("early", events.get(0).name());
        assertEquals("mid", events.get(1).name());
    }

    // ─── :exec tests ───────────────────────────────────────────────────────────

    @Test
    void testUpdatePayload() throws Exception {

        Queries.insertEvent(conn, "test", json("{\"v\":1}"), json("{\"source\":\"web\"}"),
            UUID.randomUUID(), sampleCreatedAt(), null, null, null);

        var updated = json("{\"v\":2,\"changed\":true}");
        Queries.updatePayload(conn, updated, null, 1L);

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals(updated, ev.payload());
        assertNull(ev.meta());
    }

    @Test
    void testUpdateEventDate() throws Exception {

        Queries.insertEvent(conn, "dated", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, LocalDate.of(2024, 1, 1), null);

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1L);

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate());
    }

    // ─── :execrows tests ───────────────────────────────────────────────────────

    @Test
    void testInsertEventRows() throws Exception {

        long n = Queries.insertEventRows(conn, "rowtest", json("{}"), null,
            UUID.randomUUID(), sampleCreatedAt(), null, null, null);
        assertEquals(1L, n);
    }

    // ─── projection tests ──────────────────────────────────────────────────────

    @Test
    void testFindByDate() throws Exception {

        var target = LocalDate.of(2024, 6, 15);
        Queries.insertEvent(conn, "dated", json("{}"), null, UUID.randomUUID(),
            sampleCreatedAt(), null, target, null);

        var row = Queries.findByDate(conn, target).orElseThrow();
        assertEquals("dated", row.name());
    }

    @Test
    void testFindByUuid() throws Exception {

        var docId = UUID.randomUUID();
        Queries.insertEvent(conn, "uuid-test", json("{}"), null, docId,
            sampleCreatedAt(), null, null, null);

        var row = Queries.findByUuid(conn, docId).orElseThrow();
        assertEquals("uuid-test", row.name());
    }

    // ─── count tests ───────────────────────────────────────────────────────────

    @Test
    void testCountEvents() throws Exception {

        var cutoff = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        for (int i = 0; i < 3; i++) {
            var ts = LocalDateTime.of(2024, 6, i + 1, 0, 0, 0);
            Queries.insertEvent(conn, "ev" + i, json("{}"), null, UUID.randomUUID(),
                ts, null, null, null);
        }

        var row = Queries.countEvents(conn, cutoff).orElseThrow();
        assertEquals(3L, row.total());
    }
}
