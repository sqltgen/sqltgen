package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for type overrides: Jackson JSON on MySQL.
 *
 * Each test creates a dedicated MySQL database named test_&lt;uuid&gt; to provide
 * full isolation. Requires the docker-compose MySQL service on port 13306.
 */
class RuntimeTest {

    private static final String ROOT_URL =
        System.getenv().getOrDefault("MYSQL_ROOT_URL", "jdbc:mysql://localhost:13306/sqltgen_e2e");
    private static final String TEST_BASE_URL =
        System.getenv().getOrDefault("DATABASE_URL", "jdbc:mysql://localhost:13306/");
    private static final String USER      = "sqltgen";
    private static final String PASS      = "sqltgen";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASS = "sqltgen";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Connection conn;
    private String dbName;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE `" + dbName + "`");
            s.execute("GRANT ALL ON `" + dbName + "`.* TO 'sqltgen'@'%'");
        }
        conn = DriverManager.getConnection(
            TEST_BASE_URL + dbName + "?useSSL=false&allowPublicKeyRetrieval=true", USER, PASS);
        conn.setAutoCommit(true);
        String ddl = Files.readString(Path.of("../../../../fixtures/type_overrides/mysql/schema.sql"));
        try (Statement s = conn.createStatement()) {
            for (String stmt : ddl.split(";")) {
                String t = stmt.strip();
                if (!t.isEmpty()) s.execute(t);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null) conn.close();
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS `" + dbName + "`");
        }
    }

    private static LocalDateTime sampleCreatedAt() {
        return LocalDateTime.of(2024, 6, 1, 12, 0, 0);
    }

    private JsonNode json(String raw) {
        try {
            return MAPPER.readTree(raw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ─── :one tests ────────────────────────────────────────────────────────────

    @Test
    void testInsertAndGetEvent() throws Exception {

        var payload = json("{\"type\":\"click\",\"x\":10}");
        var meta = json("{\"source\":\"web\"}");

        Queries.insertEvent(conn, "login", payload, meta, "doc-001",
            sampleCreatedAt(), LocalDateTime.of(2024, 6, 1, 14, 0, 0),
            LocalDate.of(2024, 6, 1), LocalTime.of(9, 0, 0));

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals("login", ev.name());
        assertEquals(payload, ev.payload());
        assertEquals(meta, ev.meta());
        assertEquals("doc-001", ev.docId());
        assertEquals(sampleCreatedAt(), ev.createdAt());
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

        Queries.insertEvent(conn, "alpha", json("{}"), null, "doc-1",
            sampleCreatedAt(), null, null, null);
        Queries.insertEvent(conn, "beta", json("{}"), null, "doc-2",
            sampleCreatedAt(), null, null, null);
        Queries.insertEvent(conn, "gamma", json("{}"), null, "doc-3",
            sampleCreatedAt(), null, null, null);

        var events = Queries.listEvents(conn);
        assertEquals(3, events.size());
        assertEquals("alpha", events.get(0).name());
        assertEquals("beta", events.get(1).name());
        assertEquals("gamma", events.get(2).name());
    }

    @Test
    void testGetEventsByDateRange() throws Exception {

        Queries.insertEvent(conn, "early", json("{}"), null, "doc-1",
            LocalDateTime.of(2024, 1, 1, 10, 0, 0), null, null, null);
        Queries.insertEvent(conn, "mid", json("{}"), null, "doc-2",
            sampleCreatedAt(), null, null, null);
        Queries.insertEvent(conn, "late", json("{}"), null, "doc-3",
            LocalDateTime.of(2024, 12, 1, 15, 0, 0), null, null, null);

        var events = Queries.getEventsByDateRange(conn,
            LocalDateTime.of(2024, 1, 1, 0, 0, 0), LocalDateTime.of(2024, 7, 1, 0, 0, 0));

        assertEquals(2, events.size());
        assertEquals("early", events.get(0).name());
        assertEquals("mid", events.get(1).name());
    }

    // ─── :exec tests ───────────────────────────────────────────────────────────

    @Test
    void testUpdatePayload() throws Exception {

        Queries.insertEvent(conn, "test", json("{\"v\":1}"), json("{\"source\":\"web\"}"),
            "doc-1", sampleCreatedAt(), null, null, null);

        var updated = json("{\"v\":2,\"changed\":true}");
        Queries.updatePayload(conn, updated, null, 1L);

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals(updated, ev.payload());
        assertNull(ev.meta());
    }

    @Test
    void testUpdateEventDate() throws Exception {

        Queries.insertEvent(conn, "dated", json("{}"), null, "doc-1",
            sampleCreatedAt(), null, LocalDate.of(2024, 1, 1), null);

        Queries.updateEventDate(conn, LocalDate.of(2024, 12, 31), 1L);

        var ev = Queries.getEvent(conn, 1L).orElseThrow();
        assertEquals(LocalDate.of(2024, 12, 31), ev.eventDate());
    }

    // ─── :execrows tests ───────────────────────────────────────────────────────

    @Test
    void testInsertEventRows() throws Exception {

        long n = Queries.insertEventRows(conn, "rowtest", json("{}"), null,
            "doc-1", sampleCreatedAt(), null, null, null);
        assertEquals(1L, n);
    }

    // ─── projection tests ──────────────────────────────────────────────────────

    @Test
    void testFindByDate() throws Exception {

        Queries.insertEvent(conn, "dated", json("{}"), null, "doc-1",
            sampleCreatedAt(), null, LocalDate.of(2024, 6, 15), null);

        var row = Queries.findByDate(conn, LocalDate.of(2024, 6, 15)).orElseThrow();
        assertEquals("dated", row.name());
    }

    @Test
    void testFindByDocId() throws Exception {

        Queries.insertEvent(conn, "doctest", json("{}"), null, "unique-doc-id",
            sampleCreatedAt(), null, null, null);

        var row = Queries.findByDocId(conn, "unique-doc-id").orElseThrow();
        assertEquals("doctest", row.name());
    }

    // ─── count tests ───────────────────────────────────────────────────────────

    @Test
    void testCountEvents() throws Exception {

        for (int i = 1; i <= 3; i++) {
            LocalDateTime ts = LocalDateTime.of(2024, 6, i, 0, 0, 0);
            Queries.insertEvent(conn, "ev" + i, json("{}"), null, "doc-" + i,
                ts, null, null, null);
        }

        var row = Queries.countEvents(conn, LocalDateTime.of(2024, 1, 1, 0, 0, 0)).orElseThrow();
        assertEquals(3L, row.total());
    }
}
