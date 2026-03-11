package db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_CREATE_AUTHOR =
        "INSERT INTO author (name, bio, birth_year) VALUES (?, ?, ?) RETURNING *;";
    public static Optional<Author> createAuthor(Connection conn, String name, String bio, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_AUTHOR)) {
            ps.setString(1, name);
            ps.setObject(2, bio);
            ps.setObject(3, birthYear);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, Integer.class)));
            }
        }
    }

    private static final String SQL_GET_AUTHOR =
        "SELECT id, name, bio, birth_year FROM author WHERE id = ?;";
    public static Optional<Author> getAuthor(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHOR)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, Integer.class)));
            }
        }
    }

    private static final String SQL_LIST_AUTHORS =
        "SELECT id, name, bio, birth_year FROM author ORDER BY name;";
    public static List<Author> listAuthors(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_AUTHORS)) {
            List<Author> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, Integer.class)));
            }
            return rows;
        }
    }

    private static final String SQL_UPDATE_AUTHOR_BIO =
        "UPDATE author SET bio = ? WHERE id = ? RETURNING *;";
    public static Optional<Author> updateAuthorBio(Connection conn, String bio, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO)) {
            ps.setObject(1, bio);
            ps.setLong(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, Integer.class)));
            }
        }
    }

    public record DeleteAuthorRow(
        long id,
        String name
    ) {}

    private static final String SQL_DELETE_AUTHOR =
        "DELETE FROM author WHERE id = ? RETURNING id, name;";
    public static Optional<DeleteAuthorRow> deleteAuthor(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_AUTHOR)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new DeleteAuthorRow(rs.getLong(1), rs.getString(2)));
            }
        }
    }

    private static final String SQL_CREATE_BOOK =
        "INSERT INTO book (author_id, title, genre, price, published_at) VALUES (?, ?, ?, ?, ?) RETURNING *;";
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

    private static final String SQL_GET_BOOK =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ?;";
    public static Optional<Book> getBook(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
        }
    }

    private static final String SQL_GET_BOOKS_BY_IDS =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ANY(?) ORDER BY title;";
    public static List<Book> getBooksByIds(Connection conn, List<Long> ids) throws SQLException {
        java.sql.Array arr = conn.createArrayOf("bigint", ids.toArray());
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_IDS)) {
            ps.setArray(1, arr);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_BOOKS_BY_GENRE =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = ? ORDER BY title;";
    public static List<Book> listBooksByGenre(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE)) {
            ps.setString(1, genre);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
            return rows;
        }
    }

    private static final String SQL_LIST_BOOKS_BY_GENRE_OR_ALL =
        "SELECT id, author_id, title, genre, price, published_at FROM book WHERE ? = 'all' OR genre = ? ORDER BY title;";
    public static List<Book> listBooksByGenreOrAll(Connection conn, String genre) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE_OR_ALL)) {
            ps.setString(1, genre);
            ps.setString(2, genre);
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
            return rows;
        }
    }

    public record CreateCustomerRow(
        long id
    ) {}

    private static final String SQL_CREATE_CUSTOMER =
        "INSERT INTO customer (name, email) VALUES (?, ?) RETURNING id;";
    public static Optional<CreateCustomerRow> createCustomer(Connection conn, String name, String email) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_CUSTOMER)) {
            ps.setString(1, name);
            ps.setString(2, email);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CreateCustomerRow(rs.getLong(1)));
            }
        }
    }

    public record CreateSaleRow(
        long id
    ) {}

    private static final String SQL_CREATE_SALE =
        "INSERT INTO sale (customer_id) VALUES (?) RETURNING id;";
    public static Optional<CreateSaleRow> createSale(Connection conn, long customerId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_SALE)) {
            ps.setLong(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CreateSaleRow(rs.getLong(1)));
            }
        }
    }

    private static final String SQL_ADD_SALE_ITEM =
        "INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?, ?, ?, ?);";
    public static void addSaleItem(Connection conn, long saleId, long bookId, int quantity, java.math.BigDecimal unitPrice) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ADD_SALE_ITEM)) {
            ps.setLong(1, saleId);
            ps.setLong(2, bookId);
            ps.setInt(3, quantity);
            ps.setBigDecimal(4, unitPrice);
            ps.executeUpdate();
        }
    }

    public record ListBooksWithAuthorRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price,
        java.time.LocalDate publishedAt,
        String authorName,
        String authorBio
    ) {}

    private static final String SQL_LIST_BOOKS_WITH_AUTHOR =
        "SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title;";
    public static List<ListBooksWithAuthorRow> listBooksWithAuthor(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_WITH_AUTHOR)) {
            List<ListBooksWithAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListBooksWithAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5, java.time.LocalDate.class), rs.getString(6), rs.getString(7)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_BOOKS_NEVER_ORDERED =
        "SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title;";
    public static List<Book> getBooksNeverOrdered(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_NEVER_ORDERED)) {
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
            return rows;
        }
    }

    public record GetTopSellingBooksRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price,
        Long unitsSold
    ) {}

    private static final String SQL_GET_TOP_SELLING_BOOKS =
        "WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC;";
    public static List<GetTopSellingBooksRow> getTopSellingBooks(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_TOP_SELLING_BOOKS)) {
            List<GetTopSellingBooksRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetTopSellingBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5, Long.class)));
            }
            return rows;
        }
    }

    public record GetBestCustomersRow(
        long id,
        String name,
        String email,
        java.math.BigDecimal totalSpent
    ) {}

    private static final String SQL_GET_BEST_CUSTOMERS =
        "WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC;";
    public static List<GetBestCustomersRow> getBestCustomers(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BEST_CUSTOMERS)) {
            List<GetBestCustomersRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBestCustomersRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    public record CountBooksByGenreRow(
        String genre,
        long bookCount
    ) {}

    private static final String SQL_COUNT_BOOKS_BY_GENRE =
        "SELECT genre, COUNT(*) AS book_count FROM book GROUP BY genre ORDER BY genre;";
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
        long id,
        String title,
        String genre,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_LIST_BOOKS_WITH_LIMIT =
        "SELECT id, title, genre, price FROM book ORDER BY title LIMIT ? OFFSET ?;";
    public static List<ListBooksWithLimitRow> listBooksWithLimit(Connection conn, long limit, long offset) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_BOOKS_WITH_LIMIT)) {
            ps.setLong(1, limit);
            ps.setLong(2, offset);
            List<ListBooksWithLimitRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListBooksWithLimitRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    public record SearchBooksByTitleRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_SEARCH_BOOKS_BY_TITLE =
        "SELECT id, title, genre, price FROM book WHERE title LIKE ? ORDER BY title;";
    public static List<SearchBooksByTitleRow> searchBooksByTitle(Connection conn, String title) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_SEARCH_BOOKS_BY_TITLE)) {
            ps.setString(1, title);
            List<SearchBooksByTitleRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new SearchBooksByTitleRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    public record GetBooksByPriceRangeRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_GET_BOOKS_BY_PRICE_RANGE =
        "SELECT id, title, genre, price FROM book WHERE price BETWEEN ? AND ? ORDER BY price;";
    public static List<GetBooksByPriceRangeRow> getBooksByPriceRange(Connection conn, java.math.BigDecimal price, java.math.BigDecimal price2) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_PRICE_RANGE)) {
            ps.setBigDecimal(1, price);
            ps.setBigDecimal(2, price2);
            List<GetBooksByPriceRangeRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksByPriceRangeRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    public record GetBooksInGenresRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_GET_BOOKS_IN_GENRES =
        "SELECT id, title, genre, price FROM book WHERE genre IN (?, ?, ?) ORDER BY title;";
    public static List<GetBooksInGenresRow> getBooksInGenres(Connection conn, String genre, String genre2, String genre3) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_IN_GENRES)) {
            ps.setString(1, genre);
            ps.setString(2, genre2);
            ps.setString(3, genre3);
            List<GetBooksInGenresRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksInGenresRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    public record GetBookPriceLabelRow(
        long id,
        String title,
        java.math.BigDecimal price,
        String priceLabel
    ) {}

    private static final String SQL_GET_BOOK_PRICE_LABEL =
        "SELECT id, title, price,        CASE WHEN price > ? THEN 'expensive' ELSE 'affordable' END AS price_label FROM book ORDER BY title;";
    public static List<GetBookPriceLabelRow> getBookPriceLabel(Connection conn, java.math.BigDecimal price) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_PRICE_LABEL)) {
            ps.setBigDecimal(1, price);
            List<GetBookPriceLabelRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookPriceLabelRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3), rs.getString(4)));
            }
            return rows;
        }
    }

    public record GetBookPriceOrDefaultRow(
        long id,
        String title,
        java.math.BigDecimal effectivePrice
    ) {}

    private static final String SQL_GET_BOOK_PRICE_OR_DEFAULT =
        "SELECT id, title, COALESCE(price, ?) AS effective_price FROM book ORDER BY title;";
    public static List<GetBookPriceOrDefaultRow> getBookPriceOrDefault(Connection conn, String param1) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_PRICE_OR_DEFAULT)) {
            ps.setString(1, param1);
            List<GetBookPriceOrDefaultRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookPriceOrDefaultRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3)));
            }
            return rows;
        }
    }

    private static final String SQL_DELETE_BOOK_BY_ID =
        "DELETE FROM book WHERE id = ?;";
    public static long deleteBookById(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_BOOK_BY_ID)) {
            ps.setLong(1, id);
            return ps.executeUpdate();
        }
    }

    public record GetGenresWithManyBooksRow(
        String genre,
        long bookCount
    ) {}

    private static final String SQL_GET_GENRES_WITH_MANY_BOOKS =
        "SELECT genre, COUNT(*) AS book_count FROM book GROUP BY genre HAVING COUNT(*) > ? ORDER BY genre;";
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
        long id,
        String title,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_GET_BOOKS_BY_AUTHOR_PARAM =
        "SELECT b.id, b.title, b.price FROM book b JOIN author a ON a.id = b.author_id AND a.birth_year > ? ORDER BY b.title;";
    public static List<GetBooksByAuthorParamRow> getBooksByAuthorParam(Connection conn, Integer birthYear) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_BY_AUTHOR_PARAM)) {
            ps.setObject(1, birthYear);
            List<GetBooksByAuthorParamRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksByAuthorParamRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_ALL_BOOK_FIELDS =
        "SELECT b.* FROM book b ORDER BY b.id;";
    public static List<Book> getAllBookFields(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_ALL_BOOK_FIELDS)) {
            List<Book> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate.class)));
            }
            return rows;
        }
    }

    public record GetBooksNotByAuthorRow(
        long id,
        String title,
        String genre
    ) {}

    private static final String SQL_GET_BOOKS_NOT_BY_AUTHOR =
        "SELECT id, title, genre FROM book WHERE author_id NOT IN (SELECT id FROM author WHERE name = ?) ORDER BY title;";
    public static List<GetBooksNotByAuthorRow> getBooksNotByAuthor(Connection conn, String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_NOT_BY_AUTHOR)) {
            ps.setString(1, name);
            List<GetBooksNotByAuthorRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksNotByAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record GetBooksWithRecentSalesRow(
        long id,
        String title,
        String genre
    ) {}

    private static final String SQL_GET_BOOKS_WITH_RECENT_SALES =
        "SELECT id, title, genre FROM book WHERE EXISTS (     SELECT 1 FROM sale_item si     JOIN sale s ON s.id = si.sale_id     WHERE si.book_id = book.id AND s.ordered_at > ? ) ORDER BY title;";
    public static List<GetBooksWithRecentSalesRow> getBooksWithRecentSales(Connection conn, String param1) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOKS_WITH_RECENT_SALES)) {
            ps.setString(1, param1);
            List<GetBooksWithRecentSalesRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBooksWithRecentSalesRow(rs.getLong(1), rs.getString(2), rs.getString(3)));
            }
            return rows;
        }
    }

    public record GetBookWithAuthorNameRow(
        long id,
        String title,
        Object authorName
    ) {}

    private static final String SQL_GET_BOOK_WITH_AUTHOR_NAME =
        "SELECT b.id, b.title,        (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name FROM book b ORDER BY b.title;";
    public static List<GetBookWithAuthorNameRow> getBookWithAuthorName(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_BOOK_WITH_AUTHOR_NAME)) {
            List<GetBookWithAuthorNameRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetBookWithAuthorNameRow(rs.getLong(1), rs.getString(2), rs.getObject(3)));
            }
            return rows;
        }
    }

    public record GetAuthorStatsRow(
        long id,
        String name,
        long numBooks,
        long totalSold
    ) {}

    private static final String SQL_GET_AUTHOR_STATS =
        "WITH book_counts AS (     SELECT author_id, COUNT(*) AS num_books     FROM book     GROUP BY author_id ), sale_counts AS (     SELECT b.author_id, SUM(si.quantity) AS total_sold     FROM sale_item si     JOIN book b ON b.id = si.book_id     GROUP BY b.author_id ) SELECT a.id, a.name,        COALESCE(bc.num_books, 0) AS num_books,        COALESCE(sc.total_sold, 0) AS total_sold FROM author a LEFT JOIN book_counts bc ON bc.author_id = a.id LEFT JOIN sale_counts sc ON sc.author_id = a.id ORDER BY a.name;";
    public static List<GetAuthorStatsRow> getAuthorStats(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_AUTHOR_STATS)) {
            List<GetAuthorStatsRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new GetAuthorStatsRow(rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4)));
            }
            return rows;
        }
    }

    public record ArchiveAndReturnBooksRow(
        long id,
        String title,
        String genre,
        java.math.BigDecimal price
    ) {}

    private static final String SQL_ARCHIVE_AND_RETURN_BOOKS =
        "WITH archived AS (     DELETE FROM book     WHERE published_at < ?     RETURNING id, title, genre, price ) SELECT id, title, genre, price FROM archived ORDER BY title;";
    public static List<ArchiveAndReturnBooksRow> archiveAndReturnBooks(Connection conn, java.time.LocalDate publishedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ARCHIVE_AND_RETURN_BOOKS)) {
            ps.setObject(1, publishedAt);
            List<ArchiveAndReturnBooksRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ArchiveAndReturnBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)));
            }
            return rows;
        }
    }

    private static final String SQL_GET_PRODUCT =
        "SELECT id, sku, name, active, weight_kg, rating, tags, metadata,        thumbnail, created_at, stock_count FROM product WHERE id = ?;";
    public static Optional<Product> getProduct(Connection conn, java.util.UUID id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_PRODUCT)) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Product(rs.getObject(1, java.util.UUID.class), rs.getString(2), rs.getString(3), rs.getBoolean(4), rs.getObject(5, Float.class), rs.getObject(6, Double.class), java.util.Arrays.asList((String[]) rs.getArray(7).getArray()), rs.getString(8), rs.getBytes(9), rs.getObject(10, java.time.LocalDateTime.class), rs.getShort(11)));
            }
        }
    }

    public record ListActiveProductsRow(
        java.util.UUID id,
        String sku,
        String name,
        boolean active,
        Float weightKg,
        Double rating,
        java.util.List<String> tags,
        String metadata,
        java.time.LocalDateTime createdAt,
        short stockCount
    ) {}

    private static final String SQL_LIST_ACTIVE_PRODUCTS =
        "SELECT id, sku, name, active, weight_kg, rating, tags, metadata,        created_at, stock_count FROM product WHERE active = ? ORDER BY name;";
    public static List<ListActiveProductsRow> listActiveProducts(Connection conn, boolean active) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_ACTIVE_PRODUCTS)) {
            ps.setBoolean(1, active);
            List<ListActiveProductsRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListActiveProductsRow(rs.getObject(1, java.util.UUID.class), rs.getString(2), rs.getString(3), rs.getBoolean(4), rs.getObject(5, Float.class), rs.getObject(6, Double.class), java.util.Arrays.asList((String[]) rs.getArray(7).getArray()), rs.getString(8), rs.getObject(9, java.time.LocalDateTime.class), rs.getShort(10)));
            }
            return rows;
        }
    }

    private static final String SQL_INSERT_PRODUCT =
        "INSERT INTO product (id, sku, name, active, weight_kg, rating, tags, metadata, thumbnail, stock_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *;";
    public static Optional<Product> insertProduct(Connection conn, java.util.UUID id, String sku, String name, boolean active, Float weightKg, Double rating, java.util.List<String> tags, String metadata, byte[] thumbnail, short stockCount) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_PRODUCT)) {
            ps.setObject(1, id);
            ps.setString(2, sku);
            ps.setString(3, name);
            ps.setBoolean(4, active);
            ps.setObject(5, weightKg);
            ps.setObject(6, rating);
            ps.setArray(7, conn.createArrayOf("text", tags.toArray()));
            ps.setObject(8, metadata, java.sql.Types.OTHER);
            ps.setObject(9, thumbnail);
            ps.setShort(10, stockCount);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Product(rs.getObject(1, java.util.UUID.class), rs.getString(2), rs.getString(3), rs.getBoolean(4), rs.getObject(5, Float.class), rs.getObject(6, Double.class), java.util.Arrays.asList((String[]) rs.getArray(7).getArray()), rs.getString(8), rs.getBytes(9), rs.getObject(10, java.time.LocalDateTime.class), rs.getShort(11)));
            }
        }
    }
}
