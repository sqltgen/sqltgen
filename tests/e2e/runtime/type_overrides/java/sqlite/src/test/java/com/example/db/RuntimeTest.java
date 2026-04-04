package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.example.db.queries.Queries;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for type overrides: Jackson JSON on SQLite.
 *
 * SQLite type mapping notes:
 *  - id          → int  (INTEGER PRIMARY KEY)
 *  - payload/meta → String (TEXT; JSON serialised manually via ObjectMapper)
 *  - docId       → String (TEXT)
 *  - createdAt/scheduledAt → LocalDateTime (DATETIME stored as text string)
 *  - eventDate   → LocalDate (DATE)
 *  - eventTime   → LocalTime (TIME)
 *
 * Uses an in-memory SQLite database — no external services required.
 */
class RuntimeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        conn.setAutoCommit(true);
        String ddl = java.nio.file.Files.readString(
            java.nio.file.Path.of("../../../../fixtures/type_overrides/sqlite/schema.sql"));
        try (Statement s = conn.createStatement()) {
            for (String stmt : ddl.split(";")) {
                String t = stmt.strip();
                if (!t.isEmpty()) s.execute(t);
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    private JsonNode json(String raw) {
        try { return MAPPER.readTree(raw); } catch (Exception e) { throw new RuntimeException(e); }
    }

    private String jsonStr(JsonNode node) {
        try { return MAPPER.writeValueAsString(node); } catch (Exception e) { throw new RuntimeException(e); }
    }

    // ─── :one tests ────────────────────────────────────────────────────────────

    @Test
    void testInsertAndGetEvent() throws Exception {
        var payload = json("{\"type\":\"click\",\"x\":10}");
        var meta    = json("{\"source\":\"web\"}");

        Queries.insertEvent(conn, "login",
            jsonStr(payload), jsonStr(meta), "doc-001",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), LocalDateTime.of(2024, 6, 1, 14, 0, 0),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0));

        var ev = Queries.getEvent(conn, 1).orElseThrow();
        assertEquals("login",    ev.name());
        assertEquals("doc-001",  ev.docId());
        // JSON round-trip via String storage
        assertEquals(payload, json(ev.payload()));
        assertEquals(meta,    json(ev.meta()));
        assertEquals(LocalDate.of(2024, 6, 1), ev.eventDate());
        assertEquals(LocalTime.of(9, 0, 0),    ev.eventTime());
    }

    @Test
    void testGetEventNotFound() throws Exception {
        assertTrue(Queries.getEvent(conn, 999).isEmpty());
    }

    // ─── :many tests ───────────────────────────────────────────────────────────

    @Test
    void testListEvents() throws Exception {
        Queries.insertEvent(conn, "alpha", "{}", null, "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);
        Queries.insertEvent(conn, "beta",  "{}", null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);
        Queries.insertEvent(conn, "gamma", "{}", null, "doc-3", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);

        var events = Queries.listEvents(conn);
        assertEquals(3, events.size());
        assertEquals("alpha", events.get(0).name());
        assertEquals("beta",  events.get(1).name());
        assertEquals("gamma", events.get(2).name());
    }

    @Test
    void testGetEventsByDateRange() throws Exception {
        Queries.insertEvent(conn, "early", "{}", null, "doc-1", LocalDateTime.of(2024, 1, 1, 10, 0, 0), null, null, null);
        Queries.insertEvent(conn, "mid",   "{}", null, "doc-2", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);
        Queries.insertEvent(conn, "late",  "{}", null, "doc-3", LocalDateTime.of(2024, 12, 1, 15, 0, 0), null, null, null);

        var events = Queries.getEventsByDateRange(conn,
            LocalDateTime.of(2024, 1, 1, 0, 0, 0), LocalDateTime.of(2024, 7, 1, 0, 0, 0));

        assertEquals(2, events.size());
        assertEquals("early", events.get(0).name());
        assertEquals("mid",   events.get(1).name());
    }

    // ─── :exec tests ───────────────────────────────────────────────────────────

    @Test
    void testUpdatePayload() throws Exception {
        Queries.insertEvent(conn, "test", "{\"v\":1}", "{\"source\":\"web\"}", "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);

        var updated = "{\"v\":2,\"changed\":true}";
        Queries.updatePayload(conn, updated, null, 1);

        var ev = Queries.getEvent(conn, 1).orElseThrow();
        assertEquals(json(updated), json(ev.payload()));
        assertNull(ev.meta());
    }

    @Test
    void testUpdateEventDate() throws Exception {
        Queries.insertEvent(conn, "dated", "{}", null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, LocalDate.of(2024, 1, 1), null);

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1);

        var ev = Queries.getEvent(conn, 1).orElseThrow();
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate());
    }

    // ─── :execrows tests ───────────────────────────────────────────────────────

    @Test
    void testInsertEventRows() throws Exception {
        long n = Queries.insertEventRows(conn, "rowtest", "{}", null,
            "doc-1", LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);
        assertEquals(1L, n);
    }

    // ─── projection tests ──────────────────────────────────────────────────────

    @Test
    void testFindByDate() throws Exception {
        Queries.insertEvent(conn, "dated", "{}", null, "doc-1",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, LocalDate.of(2024, 6, 15), null);

        var row = Queries.findByDate(conn, LocalDate.of(2024, 6, 15)).orElseThrow();
        assertEquals("dated", row.name());
    }

    @Test
    void testFindByDocId() throws Exception {
        Queries.insertEvent(conn, "doctest", "{}", null, "unique-doc-id",
            LocalDateTime.of(2024, 6, 1, 12, 0, 0), null, null, null);

        var row = Queries.findByDocId(conn, "unique-doc-id").orElseThrow();
        assertEquals("doctest", row.name());
    }

    // ─── count tests ───────────────────────────────────────────────────────────

    @Test
    void testCountEvents() throws Exception {
        for (int i = 1; i <= 3; i++) {
            Queries.insertEvent(conn, "ev" + i, "{}", null, "doc-" + i,
                LocalDateTime.of(2024, 6, i, 0, 0, 0), null, null, null);
        }

        var row = Queries.countEvents(conn, LocalDateTime.of(2024, 1, 1, 0, 0, 0)).orElseThrow();
        assertEquals(3L, row.total());
    }
}
