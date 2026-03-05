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

    private static final String SQL_CREATEAUTHOR =
        "INSERT INTO author (name, bio, birth_year) VALUES (?, ?, ?);";
    public static void createAuthor(Connection conn, String name, String bio, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATEAUTHOR)) {
            ps.setString(1, name);
            ps.setObject(2, bio);
            ps.setObject(3, birthYear);
            ps.executeUpdate();
        }
    }

    private static final String SQL_GETAUTHOR =
        "SELECT id, name, bio, birth_year FROM author WHERE id = ?;";
    public static Optional<Author> getAuthor(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETAUTHOR)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4)));
            }
        }
    }

    private static final String SQL_LISTAUTHORS =
        "SELECT id, name, bio, birth_year FROM author ORDER BY name;";
    public static List<Author> listAuthors(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTAUTHORS)) {
            List<Author> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Author(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATEBOOK =
        "INSERT INTO book (author_id, title, genre, price, published_at) VALUES (?, ?, ?, ?, ?);";
    public static void createBook(Connection conn, int authorId, String title, String genre, java.math.BigDecimal price, String publishedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATEBOOK)) {
            ps.setInt(1, authorId);
            ps.setString(2, title);
            ps.setString(3, genre);
            ps.setBigDecimal(4, price);
            ps.setObject(5, publishedAt);
            ps.executeUpdate();
        }
    }

    private static final String SQL_GETBOOK =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ?;";
    public static Optional<Book> getBook(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETBOOK)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getString(6)));
            }
        }
    }

    private static final String SQL_LISTBOOKSBYGENRE =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = ? ORDER BY title;";
    public static List<Book> listBooksByGenre(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTBOOKSBYGENRE)) {
            ps.setString(1, genre);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getString(6)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATECUSTOMER =
        "INSERT INTO customer (name, email) VALUES (?, ?);";
    public static void createCustomer(Connection conn, String name, String email) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATECUSTOMER)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.executeUpdate();
        }
    }

    private static final String SQL_CREATESALE =
        "INSERT INTO sale (customer_id) VALUES (?);";
    public static void createSale(Connection conn, int customerId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATESALE)) {
            ps.setInt(1, customerId);
            ps.executeUpdate();
        }
    }

    private static final String SQL_ADDSALEITEM =
        "INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?, ?, ?, ?);";
    public static void addSaleItem(Connection conn, int saleId, int bookId, int quantity, java.math.BigDecimal unitPrice) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ADDSALEITEM)) {
            ps.setInt(1, saleId);
            ps.setInt(2, bookId);
            ps.setInt(3, quantity);
            ps.setBigDecimal(4, unitPrice);
            ps.executeUpdate();
        }
    }

    public record ListBooksWithAuthorRow(
        int id,
        String title,
        String genre,
        java.math.BigDecimal price,
        String publishedAt,
        String authorName,
        String authorBio
    ) {}

    private static final String SQL_LISTBOOKSWITHAUTHOR =
        "SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title;";
    public static List<ListBooksWithAuthorRow> listBooksWithAuthor(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LISTBOOKSWITHAUTHOR)) {
            List<ListBooksWithAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListBooksWithAuthorRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getString(5), rs.getString(6), rs.getString(7)));
            }
            return rows;
        }
    }

    private static final String SQL_GETBOOKSNEVERORDERED =
        "SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title;";
    public static List<Book> getBooksNeverOrdered(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETBOOKSNEVERORDERED)) {
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getString(6)));
            }
            return rows;
        }
    }

    public record GetTopSellingBooksRow(
        int id,
        String title,
        String genre,
        java.math.BigDecimal price,
        Long unitsSold
    ) {}

    private static final String SQL_GETTOPSELLINGBOOKS =
        "WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC;";
    public static List<GetTopSellingBooksRow> getTopSellingBooks(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETTOPSELLINGBOOKS)) {
            List<GetTopSellingBooksRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetTopSellingBooksRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getLong(5)));
            }
            return rows;
        }
    }

    public record GetBestCustomersRow(
        int id,
        String name,
        String email,
        java.math.BigDecimal totalSpent
    ) {}

    private static final String SQL_GETBESTCUSTOMERS =
        "WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC;";
    public static List<GetBestCustomersRow> getBestCustomers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GETBESTCUSTOMERS)) {
            List<GetBestCustomersRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBestCustomersRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }
}
