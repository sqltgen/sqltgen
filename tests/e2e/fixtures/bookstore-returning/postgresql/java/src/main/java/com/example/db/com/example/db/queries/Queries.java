package com.example.db.queries;

import com.example.db.models.Author;
import com.example.db.models.Book;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_CREATE_AUTHOR = """
            INSERT INTO author (name, bio, birth_year)
            VALUES (?, ?, ?)
            RETURNING *;
            """;
    public static Optional<Author> createAuthor(Connection conn, String name, String bio, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_AUTHOR)) {
            ps.setString(1, name);
            ps.setObject(2, bio);
            ps.setObject(3, birthYear);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
        }
    }

    private static final String SQL_GET_AUTHOR = """
            SELECT id, name, bio, birth_year
            FROM author
            WHERE id = ?;
            """;
    public static Optional<Author> getAuthor(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHOR)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
        }
    }

    private static final String SQL_UPDATE_AUTHOR_BIO = """
            UPDATE author SET bio = ? WHERE id = ?
            RETURNING *;
            """;
    public static Optional<Author> updateAuthorBio(Connection conn, String bio, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO)) {
            ps.setObject(1, bio);
            ps.setLong(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
        }
    }

    public record DeleteAuthorRow(
        long id,
        String name
    ) {}

    private static final String SQL_DELETE_AUTHOR = """
            DELETE FROM author WHERE id = ?
            RETURNING id, name;
            """;
    public static Optional<DeleteAuthorRow> deleteAuthor(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_AUTHOR)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new DeleteAuthorRow(rs.getLong(1), rs.getString(2)));
            }
        }
    }

    private static final String SQL_CREATE_BOOK = """
            INSERT INTO book (author_id, title, genre, price, published_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING *;
            """;
    public static Optional<Book> createBook(Connection conn, long authorId, String title, String genre, java.math.BigDecimal price, java.time.LocalDate publishedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_BOOK)) {
            ps.setLong(1, authorId);
            ps.setString(2, title);
            ps.setString(3, genre);
            ps.setBigDecimal(4, price);
            ps.setObject(5, publishedAt);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
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
