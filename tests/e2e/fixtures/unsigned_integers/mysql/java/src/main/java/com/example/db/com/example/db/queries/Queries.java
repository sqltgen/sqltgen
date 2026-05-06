package com.example.db.queries;

import com.example.db.models.UnsignedValues;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class Queries {
    private Queries() {}

    private static final String SQL_INSERT_UNSIGNED_ROW = """
            INSERT INTO unsigned_values (u8_val, u16_val, u24_val, u32_val, u64_val)
            VALUES (?, ?, ?, ?, ?);
            """;
    public static void insertUnsignedRow(Connection conn, short u8Val, int u16Val, long u24Val, long u32Val, java.math.BigInteger u64Val) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_UNSIGNED_ROW)) {
            ps.setShort(1, u8Val);
            ps.setInt(2, u16Val);
            ps.setLong(3, u24Val);
            ps.setLong(4, u32Val);
            ps.setBigDecimal(5, new java.math.BigDecimal(u64Val));
            ps.executeUpdate();
        }
    }

    private static final String SQL_GET_UNSIGNED_ROWS = """
            SELECT id, u8_val, u16_val, u24_val, u32_val, u64_val
            FROM unsigned_values
            ORDER BY id;
            """;
    public static List<UnsignedValues> getUnsignedRows(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_UNSIGNED_ROWS)) {
            List<UnsignedValues> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new UnsignedValues(rs.getObject(1, java.math.BigInteger.class), rs.getShort(2), rs.getInt(3), rs.getLong(4), rs.getLong(5), rs.getObject(6, java.math.BigInteger.class)));
            }
            return rows;
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
