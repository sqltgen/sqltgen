package com.example.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_GETUSER =
        "SELECT id, name, email FROM users WHERE id = ?;";
    public static Optional<Users> getUser(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETUSER)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Users(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
        }
    }

    private static final String SQL_LISTUSERS =
        "SELECT id, name, email FROM users;";
    public static List<Users> listUsers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTUSERS)) {
            List<Users> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Users(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATEUSER =
        "INSERT INTO users (name, email) VALUES (?, ?);";
    public static void createUser(Connection conn, String name, String email) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATEUSER)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.executeUpdate();
        }
    }

    private static final String SQL_DELETEUSER =
        "DELETE FROM users WHERE id = ?;";
    public static void deleteUser(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETEUSER)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }
}
