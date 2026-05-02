package db.queries;

import db.models.BookSummaries;
import db.models.SciFiBooks;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class Queries {
    private Queries() {}

    private static final String SQL_LIST_BOOK_SUMMARIES = """
            SELECT id, title, genre, author_name
            FROM book_summaries
            ORDER BY title;
            """;
    public static List<BookSummaries> listBookSummaries(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOK_SUMMARIES)) {
            List<BookSummaries> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new BookSummaries(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_BOOK_SUMMARIES_BY_GENRE = """
            SELECT id, title, genre, author_name
            FROM book_summaries
            WHERE genre = ?
            ORDER BY title;
            """;
    public static List<BookSummaries> listBookSummariesByGenre(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOK_SUMMARIES_BY_GENRE)) {
            ps.setString(1, genre);
            List<BookSummaries> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new BookSummaries(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_SCI_FI_BOOKS = """
            SELECT id, title, author_name
            FROM sci_fi_books
            ORDER BY title;
            """;
    public static List<SciFiBooks> listSciFiBooks(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_SCI_FI_BOOKS)) {
            List<SciFiBooks> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new SciFiBooks(rs.getLong(1), rs.getString(2), rs.getString(3)));
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
