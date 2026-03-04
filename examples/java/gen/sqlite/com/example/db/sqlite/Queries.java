package com.example.db.sqlite;

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
        "SELECT id, name, email, bio FROM users WHERE id = ?;";
    public static Optional<Users> getUser(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETUSER)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Users(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
        }
    }

    private static final String SQL_LISTUSERS =
        "SELECT id, name, email, bio FROM users;";
    public static List<Users> listUsers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTUSERS)) {
            List<Users> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Users(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATEUSER =
        "INSERT INTO users (name, email, bio) VALUES (?, ?, ?);";
    public static void createUser(Connection conn, String name, String email, String bio) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATEUSER)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.setString(3, bio);
            ps.executeUpdate();
        }
    }

    private static final String SQL_DELETEUSER =
        "DELETE FROM users WHERE id = ?;";
    public static void deleteUser(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETEUSER)) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    private static final String SQL_CREATEPOST =
        "INSERT INTO posts (user_id, title, body) VALUES (?, ?, ?);";
    public static void createPost(Connection conn, int userId, String title, String body) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATEPOST)) {
            ps.setInt(1, userId);
            ps.setString(2, title);
            ps.setString(3, body);
            ps.executeUpdate();
        }
    }

    public record ListPostsByUserRow(
        int id,
        String title,
        String body
    ) {}

    private static final String SQL_LISTPOSTSBYUSER =
        "SELECT p.id, p.title, p.body FROM posts p WHERE p.user_id = ?;";
    public static List<ListPostsByUserRow> listPostsByUser(Connection conn, int userId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTPOSTSBYUSER)) {
            ps.setInt(1, userId);
            List<ListPostsByUserRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListPostsByUserRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record ListPostsWithAuthorRow(
        int id,
        String title,
        String name,
        String email
    ) {}

    private static final String SQL_LISTPOSTSWITHAUTHOR =
        "SELECT p.id, p.title, u.name, u.email FROM posts p INNER JOIN users u ON u.id = p.user_id;";
    public static List<ListPostsWithAuthorRow> listPostsWithAuthor(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTPOSTSWITHAUTHOR)) {
            List<ListPostsWithAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListPostsWithAuthorRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        }
    }

    public record ListUsersWithPostCountRow(
        String name,
        String email,
        Object postCount
    ) {}

    private static final String SQL_LISTUSERSWITHPOSTCOUNT =
        "SELECT u.name, u.email, pc.post_count FROM users u INNER JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) pc ON u.id = pc.user_id;";
    public static List<ListUsersWithPostCountRow> listUsersWithPostCount(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTUSERSWITHPOSTCOUNT)) {
            List<ListUsersWithPostCountRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListUsersWithPostCountRow(rs.getString(1), rs.getString(2), rs.getObject(3)));
            }
            return rows;
        }
    }

    public record GetActiveAuthorsRow(
        int id,
        String name,
        String email
    ) {}

    private static final String SQL_GETACTIVEAUTHORS =
        "WITH post_authors AS (     SELECT DISTINCT user_id FROM posts ) SELECT u.id, u.name, u.email FROM users u JOIN post_authors pa ON pa.user_id = u.id;";
    public static List<GetActiveAuthorsRow> getActiveAuthors(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETACTIVEAUTHORS)) {
            List<GetActiveAuthorsRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetActiveAuthorsRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }
}
