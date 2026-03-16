package db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class Queries {
    private Queries() {}
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SQL_GET_EVENT = """
            SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
            FROM event
            WHERE id = ?;
            """;
    public static Optional<Event> getEvent(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_EVENT)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Event(rs.getLong(1), rs.getString(2), objectMapper.readValue(rs.getString(3), JsonNode.class), objectMapper.readValue(rs.getString(4), JsonNode.class), rs.getObject(5, UUID.class), rs.getObject(6, LocalDateTime.class), rs.getObject(7, OffsetDateTime.class), rs.getObject(8, LocalDate.class), rs.getObject(9, LocalTime.class)));
            }
        }
    }

    private static final String SQL_LIST_EVENTS = """
            SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
            FROM event
            ORDER BY id;
            """;
    public static List<Event> listEvents(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_EVENTS)) {
            List<Event> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Event(rs.getLong(1), rs.getString(2), objectMapper.readValue(rs.getString(3), JsonNode.class), objectMapper.readValue(rs.getString(4), JsonNode.class), rs.getObject(5, UUID.class), rs.getObject(6, LocalDateTime.class), rs.getObject(7, OffsetDateTime.class), rs.getObject(8, LocalDate.class), rs.getObject(9, LocalTime.class)));
            }
            return rows;
        }
    }

    private static final String SQL_INSERT_EVENT = """
            INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """;
    public static void insertEvent(Connection conn, String name, JsonNode payload, JsonNode meta, UUID docId, LocalDateTime createdAt, OffsetDateTime scheduledAt, LocalDate eventDate, LocalTime eventTime) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_EVENT)) {
            ps.setString(1, name);
            ps.setObject(2, objectMapper.writeValueAsString(payload), java.sql.Types.OTHER);
            ps.setObject(3, objectMapper.writeValueAsString(meta), java.sql.Types.OTHER);
            ps.setObject(4, docId);
            ps.setObject(5, createdAt);
            ps.setObject(6, scheduledAt);
            ps.setObject(7, eventDate);
            ps.setObject(8, eventTime);
            ps.executeUpdate();
        }
    }

    private static final String SQL_UPDATE_PAYLOAD = """
            UPDATE event SET payload = ?, meta = ? WHERE id = ?;
            """;
    public static void updatePayload(Connection conn, JsonNode payload, JsonNode meta, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_PAYLOAD)) {
            ps.setObject(1, objectMapper.writeValueAsString(payload), java.sql.Types.OTHER);
            ps.setObject(2, objectMapper.writeValueAsString(meta), java.sql.Types.OTHER);
            ps.setLong(3, id);
            ps.executeUpdate();
        }
    }

    public record FindByDateRow(
        long id,
        String name
    ) {}

    private static final String SQL_FIND_BY_DATE = """
            SELECT id, name FROM event WHERE event_date = ?;
            """;
    public static Optional<FindByDateRow> findByDate(Connection conn, LocalDate eventDate) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_FIND_BY_DATE)) {
            ps.setObject(1, eventDate);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new FindByDateRow(rs.getLong(1), rs.getString(2)));
            }
        }
    }

    public record FindByUuidRow(
        long id,
        String name
    ) {}

    private static final String SQL_FIND_BY_UUID = """
            SELECT id, name FROM event WHERE doc_id = ?;
            """;
    public static Optional<FindByUuidRow> findByUuid(Connection conn, UUID docId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_FIND_BY_UUID)) {
            ps.setObject(1, docId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new FindByUuidRow(rs.getLong(1), rs.getString(2)));
            }
        }
    }

    private static Boolean getNullableBoolean(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        boolean v = rs.getBoolean(col);
        return rs.wasNull() ? null : v;
    }
    private static Short getNullableShort(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        short v = rs.getShort(col);
        return rs.wasNull() ? null : v;
    }
    private static Integer getNullableInt(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        int v = rs.getInt(col);
        return rs.wasNull() ? null : v;
    }
    private static Long getNullableLong(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
    private static Float getNullableFloat(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : v;
    }
    private static Double getNullableDouble(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }
}
