package com.example.db.queries;

import com.example.db.models.Record;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

public final class Queries {
    private Queries() {}

    private static final String SQL_INSERT_RECORD = """
            INSERT INTO record (label, timestamps, uuids)
            VALUES (?, ?, ?);
            """;
    public static void insertRecord(Connection conn, String label, java.util.List<LocalDateTime> timestamps, java.util.List<UUID> uuids) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_RECORD)) {
            ps.setString(1, label);
            ps.setArray(2, conn.createArrayOf("timestamp", timestamps.toArray()));
            ps.setArray(3, conn.createArrayOf("uuid", uuids.toArray()));
            ps.executeUpdate();
        }
    }

    private static final String SQL_GET_RECORD = """
            SELECT id, label, timestamps, uuids
            FROM record
            WHERE id = ?;
            """;
    public static Optional<Record> getRecord(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_RECORD)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Record(rs.getLong(1), rs.getString(2), java.util.Arrays.stream((Object[]) rs.getArray(3).getArray()).map(it -> ((java.sql.Timestamp) it).toLocalDateTime()).collect(java.util.stream.Collectors.toList()), java.util.Arrays.stream((Object[]) rs.getArray(4).getArray()).map(it -> (java.util.UUID) it).collect(java.util.stream.Collectors.toList())));
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
