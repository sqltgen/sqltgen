package db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

public final class QueriesDs {
    private final DataSource dataSource;

    public QueriesDs(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Optional<Author> createAuthor(String name, String bio, Integer birthYear) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.createAuthor(conn, name, bio, birthYear);
        }
    }

    public Optional<Author> getAuthor(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAuthor(conn, id);
        }
    }

    public List<Author> listAuthors() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listAuthors(conn);
        }
    }

    public Optional<Author> updateAuthorBio(String bio, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.updateAuthorBio(conn, bio, id);
        }
    }

    public Optional<Queries.DeleteAuthorRow> deleteAuthor(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.deleteAuthor(conn, id);
        }
    }

    public Optional<Book> createBook(long authorId, String title, String genre, java.math.BigDecimal price, java.time.LocalDate publishedAt) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.createBook(conn, authorId, title, genre, price, publishedAt);
        }
    }

    public Optional<Book> getBook(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBook(conn, id);
        }
    }

    public List<Book> getBooksByIds(List<Long> ids) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksByIds(conn, ids);
        }
    }

    public List<Book> listBooksByGenre(String genre) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBooksByGenre(conn, genre);
        }
    }

    public List<Book> listBooksByGenreOrAll(String genre) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBooksByGenreOrAll(conn, genre);
        }
    }

    public Optional<Queries.CreateCustomerRow> createCustomer(String name, String email) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.createCustomer(conn, name, email);
        }
    }

    public Optional<Queries.CreateSaleRow> createSale(long customerId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.createSale(conn, customerId);
        }
    }

    public void addSaleItem(long saleId, long bookId, int quantity, java.math.BigDecimal unitPrice) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.addSaleItem(conn, saleId, bookId, quantity, unitPrice);
        }
    }

    public List<Queries.ListBooksWithAuthorRow> listBooksWithAuthor() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBooksWithAuthor(conn);
        }
    }

    public List<Book> getBooksNeverOrdered() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksNeverOrdered(conn);
        }
    }

    public List<Queries.GetTopSellingBooksRow> getTopSellingBooks() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getTopSellingBooks(conn);
        }
    }

    public List<Queries.GetBestCustomersRow> getBestCustomers() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBestCustomers(conn);
        }
    }

    public List<Queries.CountBooksByGenreRow> countBooksByGenre() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.countBooksByGenre(conn);
        }
    }

    public List<Queries.ListBooksWithLimitRow> listBooksWithLimit(long limit, long offset) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBooksWithLimit(conn, limit, offset);
        }
    }

    public List<Queries.SearchBooksByTitleRow> searchBooksByTitle(String title) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.searchBooksByTitle(conn, title);
        }
    }

    public List<Queries.GetBooksByPriceRangeRow> getBooksByPriceRange(java.math.BigDecimal price, java.math.BigDecimal price) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksByPriceRange(conn, price, price);
        }
    }

    public List<Queries.GetBooksInGenresRow> getBooksInGenres(String genre, String genre, String genre) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksInGenres(conn, genre, genre, genre);
        }
    }

    public List<Queries.GetBookPriceLabelRow> getBookPriceLabel(java.math.BigDecimal price) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookPriceLabel(conn, price);
        }
    }

    public List<Queries.GetBookPriceOrDefaultRow> getBookPriceOrDefault(String param1) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookPriceOrDefault(conn, param1);
        }
    }
}
