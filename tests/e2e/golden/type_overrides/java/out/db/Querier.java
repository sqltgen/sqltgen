package db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static com.fasterxml.jackson.databind.JsonNode parseJson(String raw) { try { return raw == null ? null : objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode.class); } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new RuntimeException(e); } }
    private static String toJson(com.fasterxml.jackson.databind.JsonNode value) { if (value == null) return null; try { return objectMapper.writeValueAsString(value); } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new RuntimeException(e); } }

    public Querier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Optional<Event> getEvent(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getEvent(conn, id);
        }
    }

    public List<Event> listEvents() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listEvents(conn);
        }
    }

    public void insertEvent(String name, JsonNode payload, JsonNode meta, UUID docId, LocalDateTime createdAt, OffsetDateTime scheduledAt, LocalDate eventDate, LocalTime eventTime) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.insertEvent(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime);
        }
    }

    public void updatePayload(JsonNode payload, JsonNode meta, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.updatePayload(conn, payload, meta, id);
        }
    }

    public Optional<Queries.FindByDateRow> findByDate(LocalDate eventDate) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.findByDate(conn, eventDate);
        }
    }

    public Optional<Queries.FindByUuidRow> findByUuid(UUID docId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.findByUuid(conn, docId);
        }
    }

    public long insertEventRows(String name, JsonNode payload, JsonNode meta, UUID docId, LocalDateTime createdAt, OffsetDateTime scheduledAt, LocalDate eventDate, LocalTime eventTime) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.insertEventRows(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime);
        }
    }

    public List<Event> getEventsByDateRange(LocalDateTime createdAt, LocalDateTime createdAt2) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getEventsByDateRange(conn, createdAt, createdAt2);
        }
    }

    public Optional<Queries.CountEventsRow> countEvents(LocalDateTime createdAt) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.countEvents(conn, createdAt);
        }
    }

    public void updateEventDate(LocalDate eventDate, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.updateEventDate(conn, eventDate, id);
        }
    }
}
