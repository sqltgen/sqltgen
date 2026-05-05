package com.example.db.queries;

import com.example.db.models.Author;
import com.example.db.models.Book;
import com.example.db.models.Product;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_CREATE_AUTHOR = """
            INSERT INTO author (name, bio, birth_year)
            VALUES (?, ?, ?);
            """;
    public static void createAuthor(Connection conn, String name, String bio, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_AUTHOR)) {
            ps.setString(1, name);
            ps.setObject(2, bio);
            ps.setObject(3, birthYear);
            ps.executeUpdate();
        }
    }

    private static final String SQL_GET_AUTHOR = """
            SELECT id, name, bio, birth_year
            FROM author
            WHERE id = ?;
            """;
    public static Optional<Author> getAuthor(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHOR)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getInt(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
        }
    }

    private static final String SQL_LIST_AUTHORS = """
            SELECT id, name, bio, birth_year
            FROM author
            ORDER BY name;
            """;
    public static List<Author> listAuthors(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_AUTHORS)) {
            List<Author> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Author(rs.getInt(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATE_BOOK = """
            INSERT INTO book (author_id, title, genre, price, published_at)
            VALUES (?, ?, ?, ?, ?);
            """;
    public static void createBook(Connection conn, int authorId, String title, String genre, double price, String publishedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_BOOK)) {
            ps.setInt(1, authorId);
            ps.setString(2, title);
            ps.setString(3, genre);
            ps.setDouble(4, price);
            ps.setObject(5, publishedAt);
            ps.executeUpdate();
        }
    }

    private static final String SQL_GET_BOOK = """
            SELECT id, author_id, title, genre, price, published_at
            FROM book
            WHERE id = ?;
            """;
    public static Optional<Book> getBook(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
        }
    }

    private static final String SQL_GET_BOOKS_BY_IDS = """
            SELECT id, author_id, title, genre, price, published_at
            FROM book
            WHERE id IN (SELECT value FROM json_each(?))
            ORDER BY title;
            """;
    public static List<Book> getBooksByIds(Connection conn, List<Long> ids) throws SQLException {
        String json = "[" + ids.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(",")) + "]";
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_IDS)) {
            ps.setString(1, json);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_BOOKS_BY_GENRE = """
            SELECT id, author_id, title, genre, price, published_at
            FROM book
            WHERE genre = ?
            ORDER BY title;
            """;
    public static List<Book> listBooksByGenre(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE)) {
            ps.setString(1, genre);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_BOOKS_BY_GENRE_OR_ALL = """
            SELECT id, author_id, title, genre, price, published_at
            FROM book
            WHERE ? IS NULL OR genre = ?
            ORDER BY title;
            """;
    public static List<Book> listBooksByGenreOrAll(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE_OR_ALL)) {
            ps.setObject(1, genre);
            ps.setObject(2, genre);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATE_CUSTOMER = """
            INSERT INTO customer (name, email)
            VALUES (?, ?);
            """;
    public static void createCustomer(Connection conn, String name, String email) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_CUSTOMER)) {
            ps.setString(1, name);
            ps.setString(2, email);
            ps.executeUpdate();
        }
    }

    private static final String SQL_CREATE_SALE = """
            INSERT INTO sale (customer_id)
            VALUES (?);
            """;
    public static void createSale(Connection conn, int customerId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_SALE)) {
            ps.setInt(1, customerId);
            ps.executeUpdate();
        }
    }

    private static final String SQL_ADD_SALE_ITEM = """
            INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
            VALUES (?, ?, ?, ?);
            """;
    public static void addSaleItem(Connection conn, int saleId, int bookId, int quantity, double unitPrice) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ADD_SALE_ITEM)) {
            ps.setInt(1, saleId);
            ps.setInt(2, bookId);
            ps.setInt(3, quantity);
            ps.setDouble(4, unitPrice);
            ps.executeUpdate();
        }
    }

    public record ListBooksWithAuthorRow(
        int id,
        String title,
        String genre,
        double price,
        String publishedAt,
        String authorName,
        String authorBio
    ) {}

    private static final String SQL_LIST_BOOKS_WITH_AUTHOR = """
            SELECT b.id, b.title, b.genre, b.price, b.published_at,
                   a.name AS author_name, a.bio AS author_bio
            FROM book b
            JOIN author a ON a.id = b.author_id
            ORDER BY b.title;
            """;
    public static List<ListBooksWithAuthorRow> listBooksWithAuthor(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_WITH_AUTHOR)) {
            List<ListBooksWithAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListBooksWithAuthorRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getString(5), rs.getString(6), rs.getString(7)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_BOOKS_NEVER_ORDERED = """
            SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
            FROM book b
            LEFT JOIN sale_item si ON si.book_id = b.id
            WHERE si.id IS NULL
            ORDER BY b.title;
            """;
    public static List<Book> getBooksNeverOrdered(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_NEVER_ORDERED)) {
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
            return rows;
        }
    }

    public record GetTopSellingBooksRow(
        int id,
        String title,
        String genre,
        double price,
        Long unitsSold
    ) {}

    private static final String SQL_GET_TOP_SELLING_BOOKS = """
            WITH book_sales AS (
                SELECT book_id,
                       SUM(quantity) AS units_sold
                FROM sale_item
                GROUP BY book_id
            )
            SELECT b.id, b.title, b.genre, b.price,
                   bs.units_sold
            FROM book b
            JOIN book_sales bs ON bs.book_id = b.id
            ORDER BY bs.units_sold DESC;
            """;
    public static List<GetTopSellingBooksRow> getTopSellingBooks(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_TOP_SELLING_BOOKS)) {
            List<GetTopSellingBooksRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetTopSellingBooksRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4), getNullableLong(rs, 5)));
            }
            return rows;
        }
    }

    public record GetBestCustomersRow(
        int id,
        String name,
        String email,
        Double totalSpent
    ) {}

    private static final String SQL_GET_BEST_CUSTOMERS = """
            WITH customer_spend AS (
                SELECT s.customer_id,
                       SUM(si.quantity * si.unit_price) AS total_spent
                FROM sale s
                JOIN sale_item si ON si.sale_id = s.id
                GROUP BY s.customer_id
            )
            SELECT c.id, c.name, c.email,
                   cs.total_spent
            FROM customer c
            JOIN customer_spend cs ON cs.customer_id = c.id
            ORDER BY cs.total_spent DESC;
            """;
    public static List<GetBestCustomersRow> getBestCustomers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BEST_CUSTOMERS)) {
            List<GetBestCustomersRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBestCustomersRow(rs.getInt(1), rs.getString(2), rs.getString(3), getNullableDouble(rs, 4)));
            }
            return rows;
        }
    }

    public record CountBooksByGenreRow(
        String genre,
        long bookCount
    ) {}

    private static final String SQL_COUNT_BOOKS_BY_GENRE = """
            SELECT genre, COUNT(*) AS book_count
            FROM book
            GROUP BY genre
            ORDER BY genre;
            """;
    public static List<CountBooksByGenreRow> countBooksByGenre(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_COUNT_BOOKS_BY_GENRE)) {
            List<CountBooksByGenreRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new CountBooksByGenreRow(rs.getString(1), rs.getLong(2)));
            }
            return rows;
        }
    }

    public record ListBooksWithLimitRow(
        int id,
        String title,
        String genre,
        double price
    ) {}

    private static final String SQL_LIST_BOOKS_WITH_LIMIT = """
            SELECT id, title, genre, price
            FROM book
            ORDER BY title
            LIMIT ? OFFSET ?;
            """;
    public static List<ListBooksWithLimitRow> listBooksWithLimit(Connection conn, long limit, long offset) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_WITH_LIMIT)) {
            ps.setLong(1, limit);
            ps.setLong(2, offset);
            List<ListBooksWithLimitRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListBooksWithLimitRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4)));
            }
            return rows;
        }
    }

    public record SearchBooksByTitleRow(
        int id,
        String title,
        String genre,
        double price
    ) {}

    private static final String SQL_SEARCH_BOOKS_BY_TITLE = """
            SELECT id, title, genre, price
            FROM book
            WHERE title LIKE ?
            ORDER BY title;
            """;
    public static List<SearchBooksByTitleRow> searchBooksByTitle(Connection conn, String title) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_SEARCH_BOOKS_BY_TITLE)) {
            ps.setString(1, title);
            List<SearchBooksByTitleRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new SearchBooksByTitleRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4)));
            }
            return rows;
        }
    }

    public record GetBooksByPriceRangeRow(
        int id,
        String title,
        String genre,
        double price
    ) {}

    private static final String SQL_GET_BOOKS_BY_PRICE_RANGE = """
            SELECT id, title, genre, price
            FROM book
            WHERE price BETWEEN ? AND ?
            ORDER BY price;
            """;
    public static List<GetBooksByPriceRangeRow> getBooksByPriceRange(Connection conn, double price, double price2) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_PRICE_RANGE)) {
            ps.setDouble(1, price);
            ps.setDouble(2, price2);
            List<GetBooksByPriceRangeRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksByPriceRangeRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4)));
            }
            return rows;
        }
    }

    public record GetBooksInGenresRow(
        int id,
        String title,
        String genre,
        double price
    ) {}

    private static final String SQL_GET_BOOKS_IN_GENRES = """
            SELECT id, title, genre, price
            FROM book
            WHERE genre IN (?, ?, ?)
            ORDER BY title;
            """;
    public static List<GetBooksInGenresRow> getBooksInGenres(Connection conn, String genre, String genre2, String genre3) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_IN_GENRES)) {
            ps.setString(1, genre);
            ps.setString(2, genre2);
            ps.setString(3, genre3);
            List<GetBooksInGenresRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksInGenresRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4)));
            }
            return rows;
        }
    }

    public record GetBookPriceLabelRow(
        int id,
        String title,
        double price,
        String priceLabel
    ) {}

    private static final String SQL_GET_BOOK_PRICE_LABEL = """
            SELECT id, title, price,
                   CASE WHEN price > ? THEN 'expensive' ELSE 'affordable' END AS price_label
            FROM book
            ORDER BY title;
            """;
    public static List<GetBookPriceLabelRow> getBookPriceLabel(Connection conn, double price) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_PRICE_LABEL)) {
            ps.setDouble(1, price);
            List<GetBookPriceLabelRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookPriceLabelRow(rs.getInt(1), rs.getString(2), rs.getDouble(3), rs.getString(4)));
            }
            return rows;
        }
    }

    public record GetBookPriceOrDefaultRow(
        int id,
        String title,
        double effectivePrice
    ) {}

    private static final String SQL_GET_BOOK_PRICE_OR_DEFAULT = """
            SELECT id, title, COALESCE(price, ?) AS effective_price
            FROM book
            ORDER BY title;
            """;
    public static List<GetBookPriceOrDefaultRow> getBookPriceOrDefault(Connection conn, Double price) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_PRICE_OR_DEFAULT)) {
            ps.setObject(1, price);
            List<GetBookPriceOrDefaultRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookPriceOrDefaultRow(rs.getInt(1), rs.getString(2), rs.getDouble(3)));
            }
            return rows;
        }
    }

    private static final String SQL_DELETE_BOOK_BY_ID = """
            DELETE FROM book WHERE id = ?;
            """;
    public static long deleteBookById(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_BOOK_BY_ID)) {
            ps.setInt(1, id);
            return ps.executeUpdate();
        }
    }

    public record GetGenresWithManyBooksRow(
        String genre,
        long bookCount
    ) {}

    private static final String SQL_GET_GENRES_WITH_MANY_BOOKS = """
            SELECT genre, COUNT(*) AS book_count
            FROM book
            GROUP BY genre
            HAVING COUNT(*) > ?
            ORDER BY genre;
            """;
    public static List<GetGenresWithManyBooksRow> getGenresWithManyBooks(Connection conn, long count) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_GENRES_WITH_MANY_BOOKS)) {
            ps.setLong(1, count);
            List<GetGenresWithManyBooksRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetGenresWithManyBooksRow(rs.getString(1), rs.getLong(2)));
            }
            return rows;
        }
    }

    public record GetBooksByAuthorParamRow(
        int id,
        String title,
        double price
    ) {}

    private static final String SQL_GET_BOOKS_BY_AUTHOR_PARAM = """
            SELECT b.id, b.title, b.price
            FROM book b
            JOIN author a ON a.id = b.author_id AND a.birth_year > ?
            ORDER BY b.title;
            """;
    public static List<GetBooksByAuthorParamRow> getBooksByAuthorParam(Connection conn, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_AUTHOR_PARAM)) {
            ps.setObject(1, birthYear);
            List<GetBooksByAuthorParamRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksByAuthorParamRow(rs.getInt(1), rs.getString(2), rs.getDouble(3)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_ALL_BOOK_FIELDS = """
            SELECT b.*
            FROM book b
            ORDER BY b.id;
            """;
    public static List<Book> getAllBookFields(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_ALL_BOOK_FIELDS)) {
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getInt(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getString(6)));
            }
            return rows;
        }
    }

    public record GetBooksNotByAuthorRow(
        int id,
        String title,
        String genre
    ) {}

    private static final String SQL_GET_BOOKS_NOT_BY_AUTHOR = """
            SELECT id, title, genre
            FROM book
            WHERE author_id NOT IN (SELECT id FROM author WHERE name = ?)
            ORDER BY title;
            """;
    public static List<GetBooksNotByAuthorRow> getBooksNotByAuthor(Connection conn, String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_NOT_BY_AUTHOR)) {
            ps.setString(1, name);
            List<GetBooksNotByAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksNotByAuthorRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record GetBooksWithRecentSalesRow(
        int id,
        String title,
        String genre
    ) {}

    private static final String SQL_GET_BOOKS_WITH_RECENT_SALES = """
            SELECT id, title, genre
            FROM book
            WHERE EXISTS (
                SELECT 1 FROM sale_item si
                JOIN sale s ON s.id = si.sale_id
                WHERE si.book_id = book.id AND s.ordered_at > ?
            )
            ORDER BY title;
            """;
    public static List<GetBooksWithRecentSalesRow> getBooksWithRecentSales(Connection conn, java.time.LocalDateTime orderedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_WITH_RECENT_SALES)) {
            ps.setObject(1, orderedAt);
            List<GetBooksWithRecentSalesRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksWithRecentSalesRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record GetBookWithAuthorNameRow(
        int id,
        String title,
        String authorName
    ) {}

    private static final String SQL_GET_BOOK_WITH_AUTHOR_NAME = """
            SELECT b.id, b.title,
                   (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name
            FROM book b
            ORDER BY b.title;
            """;
    public static List<GetBookWithAuthorNameRow> getBookWithAuthorName(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_WITH_AUTHOR_NAME)) {
            List<GetBookWithAuthorNameRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookWithAuthorNameRow(rs.getInt(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record GetAuthorStatsRow(
        int id,
        String name,
        long numBooks,
        long totalSold
    ) {}

    private static final String SQL_GET_AUTHOR_STATS = """
            WITH book_counts AS (
                SELECT author_id, COUNT(*) AS num_books
                FROM book
                GROUP BY author_id
            ),
            sale_counts AS (
                SELECT b.author_id, SUM(si.quantity) AS total_sold
                FROM sale_item si
                JOIN book b ON b.id = si.book_id
                GROUP BY b.author_id
            )
            SELECT a.id, a.name,
                   COALESCE(bc.num_books, 0) AS num_books,
                   COALESCE(sc.total_sold, 0) AS total_sold
            FROM author a
            LEFT JOIN book_counts bc ON bc.author_id = a.id
            LEFT JOIN sale_counts sc ON sc.author_id = a.id
            ORDER BY a.name;
            """;
    public static List<GetAuthorStatsRow> getAuthorStats(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHOR_STATS)) {
            List<GetAuthorStatsRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetAuthorStatsRow(rs.getInt(1), rs.getString(2), rs.getLong(3), rs.getLong(4)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_PRODUCT = """
            SELECT id, sku, name, active, weight_kg, rating, metadata,
                   thumbnail, created_at, stock_count
            FROM product
            WHERE id = ?;
            """;
    public static Optional<Product> getProduct(Connection conn, String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_PRODUCT)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Product(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), getNullableFloat(rs, 5), getNullableFloat(rs, 6), rs.getString(7), rs.getBytes(8), rs.getString(9), rs.getInt(10)));
            }
        }
    }

    public record ListActiveProductsRow(
        String id,
        String sku,
        String name,
        int active,
        Float weightKg,
        Float rating,
        String metadata,
        String createdAt,
        int stockCount
    ) {}

    private static final String SQL_LIST_ACTIVE_PRODUCTS = """
            SELECT id, sku, name, active, weight_kg, rating, metadata,
                   created_at, stock_count
            FROM product
            WHERE active = ?
            ORDER BY name;
            """;
    public static List<ListActiveProductsRow> listActiveProducts(Connection conn, int active) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_ACTIVE_PRODUCTS)) {
            ps.setInt(1, active);
            List<ListActiveProductsRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListActiveProductsRow(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), getNullableFloat(rs, 5), getNullableFloat(rs, 6), rs.getString(7), rs.getString(8), rs.getInt(9)));
            }
            return rows;
        }
    }

    public record GetAuthorsWithNullBioRow(
        int id,
        String name,
        Integer birthYear
    ) {}

    private static final String SQL_GET_AUTHORS_WITH_NULL_BIO = """
            SELECT id, name, birth_year
            FROM author
            WHERE bio IS NULL
            ORDER BY name;
            """;
    public static List<GetAuthorsWithNullBioRow> getAuthorsWithNullBio(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHORS_WITH_NULL_BIO)) {
            List<GetAuthorsWithNullBioRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetAuthorsWithNullBioRow(rs.getInt(1), rs.getString(2), getNullableInt(rs, 3)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_AUTHORS_WITH_BIO = """
            SELECT id, name, bio, birth_year
            FROM author
            WHERE bio IS NOT NULL
            ORDER BY name;
            """;
    public static List<Author> getAuthorsWithBio(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHORS_WITH_BIO)) {
            List<Author> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Author(rs.getInt(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)));
            }
            return rows;
        }
    }

    public record GetBooksPublishedBetweenRow(
        int id,
        String title,
        String genre,
        double price,
        String publishedAt
    ) {}

    private static final String SQL_GET_BOOKS_PUBLISHED_BETWEEN = """
            SELECT id, title, genre, price, published_at
            FROM book
            WHERE published_at IS NOT NULL
              AND published_at BETWEEN ? AND ?
            ORDER BY published_at;
            """;
    public static List<GetBooksPublishedBetweenRow> getBooksPublishedBetween(Connection conn, String publishedAt, String publishedAt2) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_PUBLISHED_BETWEEN)) {
            ps.setObject(1, publishedAt);
            ps.setObject(2, publishedAt2);
            List<GetBooksPublishedBetweenRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksPublishedBetweenRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getString(5)));
            }
            return rows;
        }
    }

    public record GetDistinctGenresRow(
        String genre
    ) {}

    private static final String SQL_GET_DISTINCT_GENRES = """
            SELECT DISTINCT genre
            FROM book
            ORDER BY genre;
            """;
    public static List<GetDistinctGenresRow> getDistinctGenres(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_DISTINCT_GENRES)) {
            List<GetDistinctGenresRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetDistinctGenresRow(rs.getString(1)));
            }
            return rows;
        }
    }

    public record GetBooksWithSalesCountRow(
        int id,
        String title,
        String genre,
        long totalQuantity
    ) {}

    private static final String SQL_GET_BOOKS_WITH_SALES_COUNT = """
            SELECT b.id, b.title, b.genre,
                   COALESCE(SUM(si.quantity), 0) AS total_quantity
            FROM book b
            LEFT JOIN sale_item si ON si.book_id = b.id
            GROUP BY b.id, b.title, b.genre
            ORDER BY total_quantity DESC, b.title;
            """;
    public static List<GetBooksWithSalesCountRow> getBooksWithSalesCount(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_WITH_SALES_COUNT)) {
            List<GetBooksWithSalesCountRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksWithSalesCountRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
            }
            return rows;
        }
    }

    public record CountSaleItemsRow(
        long itemCount
    ) {}

    private static final String SQL_COUNT_SALE_ITEMS = """
            SELECT COUNT(*) AS item_count
            FROM sale_item
            WHERE sale_id = ?;
            """;
    public static Optional<CountSaleItemsRow> countSaleItems(Connection conn, int saleId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_COUNT_SALE_ITEMS)) {
            ps.setInt(1, saleId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CountSaleItemsRow(rs.getLong(1)));
            }
        }
    }

    private static final String SQL_UPDATE_AUTHOR_BIO = """
            UPDATE author SET bio = ? WHERE id = ?;
            """;
    public static void updateAuthorBio(Connection conn, String bio, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO)) {
            ps.setObject(1, bio);
            ps.setInt(2, id);
            ps.executeUpdate();
        }
    }

    private static final String SQL_DELETE_AUTHOR = """
            DELETE FROM author WHERE id = ?;
            """;
    public static void deleteAuthor(Connection conn, int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_AUTHOR)) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    private static final String SQL_INSERT_PRODUCT = """
            INSERT INTO product (id, sku, name, active, weight_kg, rating, metadata, thumbnail, stock_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """;
    public static void insertProduct(Connection conn, String id, String sku, String name, int active, Float weightKg, Float rating, String metadata, byte[] thumbnail, int stockCount) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_PRODUCT)) {
            ps.setString(1, id);
            ps.setString(2, sku);
            ps.setString(3, name);
            ps.setInt(4, active);
            ps.setObject(5, weightKg);
            ps.setObject(6, rating);
            ps.setObject(7, metadata);
            ps.setObject(8, thumbnail);
            ps.setInt(9, stockCount);
            ps.executeUpdate();
        }
    }

    private static final String SQL_UPSERT_PRODUCT = """
            INSERT INTO product (id, sku, name, active, metadata, stock_count)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE
                SET name        = EXCLUDED.name,
                    active      = EXCLUDED.active,
                    metadata    = EXCLUDED.metadata,
                    stock_count = EXCLUDED.stock_count;
            """;
    public static void upsertProduct(Connection conn, String id, String sku, String name, int active, String metadata, int stockCount) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPSERT_PRODUCT)) {
            ps.setString(1, id);
            ps.setString(2, sku);
            ps.setString(3, name);
            ps.setInt(4, active);
            ps.setObject(5, metadata);
            ps.setInt(6, stockCount);
            ps.executeUpdate();
        }
    }

    public record GetSaleItemQuantityAggregatesRow(
        Integer minQty,
        Integer maxQty,
        Long sumQty,
        Double avgQty
    ) {}

    private static final String SQL_GET_SALE_ITEM_QUANTITY_AGGREGATES = """
            SELECT MIN(quantity)  AS min_qty,
                   MAX(quantity)  AS max_qty,
                   SUM(quantity)  AS sum_qty,
                   AVG(quantity)  AS avg_qty
            FROM sale_item;
            """;
    public static Optional<GetSaleItemQuantityAggregatesRow> getSaleItemQuantityAggregates(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_SALE_ITEM_QUANTITY_AGGREGATES)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new GetSaleItemQuantityAggregatesRow(getNullableInt(rs, 1), getNullableInt(rs, 2), getNullableLong(rs, 3), getNullableDouble(rs, 4)));
            }
        }
    }

    public record GetBookPriceAggregatesRow(
        Double minPrice,
        Double maxPrice,
        Double sumPrice,
        Double avgPrice
    ) {}

    private static final String SQL_GET_BOOK_PRICE_AGGREGATES = """
            SELECT MIN(price)  AS min_price,
                   MAX(price)  AS max_price,
                   SUM(price)  AS sum_price,
                   AVG(price)  AS avg_price
            FROM book;
            """;
    public static Optional<GetBookPriceAggregatesRow> getBookPriceAggregates(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_PRICE_AGGREGATES)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new GetBookPriceAggregatesRow(getNullableDouble(rs, 1), getNullableDouble(rs, 2), getNullableDouble(rs, 3), getNullableDouble(rs, 4)));
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
