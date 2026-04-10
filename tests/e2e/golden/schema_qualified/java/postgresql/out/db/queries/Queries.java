package db.queries;

import db.models.Internal_AuditLog;
import db.models.Users;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_GET_USER = """
            SELECT * FROM public.users WHERE id = ?;
            """;
    public static Optional<Users> getUser(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_USER)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Users(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
        }
    }

    private static final String SQL_LIST_AUDIT_LOGS = """
            SELECT * FROM internal.audit_log ORDER BY created_at DESC;
            """;
    public static List<Internal_AuditLog> listAuditLogs(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_AUDIT_LOGS)) {
            List<Internal_AuditLog> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Internal_AuditLog(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getObject(4, java.time.LocalDateTime.class)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATE_AUDIT_LOG = """
            INSERT INTO internal.audit_log (user_id, action) VALUES (?, ?);
            """;
    public static void createAuditLog(Connection conn, long userId, String action) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_AUDIT_LOG)) {
            ps.setLong(1, userId);
            ps.setString(2, action);
            ps.executeUpdate();
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
