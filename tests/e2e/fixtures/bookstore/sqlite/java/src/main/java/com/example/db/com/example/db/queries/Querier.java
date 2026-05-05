package com.example.db.queries;

import com.example.db.models.Author;
import com.example.db.models.Book;
import com.example.db.models.Product;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;

    public Querier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void createAuthor(String name, String bio, Integer birthYear) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.createAuthor(conn, name, bio, birthYear);
        }
    }

    public Optional<Author> getAuthor(int id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAuthor(conn, id);
        }
    }

    public List<Author> listAuthors() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listAuthors(conn);
        }
    }

    public void createBook(int authorId, String title, String genre, double price, String publishedAt) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.createBook(conn, authorId, title, genre, price, publishedAt);
        }
    }

    public Optional<Book> getBook(int id) throws SQLException {
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

    public void createCustomer(String name, String email) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.createCustomer(conn, name, email);
        }
    }

    public void createSale(int customerId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.createSale(conn, customerId);
        }
    }

    public void addSaleItem(int saleId, int bookId, int quantity, double unitPrice) throws SQLException {
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

    public List<Queries.GetBooksByPriceRangeRow> getBooksByPriceRange(double price, double price2) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksByPriceRange(conn, price, price2);
        }
    }

    public List<Queries.GetBooksInGenresRow> getBooksInGenres(String genre, String genre2, String genre3) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksInGenres(conn, genre, genre2, genre3);
        }
    }

    public List<Queries.GetBookPriceLabelRow> getBookPriceLabel(double price) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookPriceLabel(conn, price);
        }
    }

    public List<Queries.GetBookPriceOrDefaultRow> getBookPriceOrDefault(Double price) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookPriceOrDefault(conn, price);
        }
    }

    public long deleteBookById(int id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.deleteBookById(conn, id);
        }
    }

    public List<Queries.GetGenresWithManyBooksRow> getGenresWithManyBooks(long count) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getGenresWithManyBooks(conn, count);
        }
    }

    public List<Queries.GetBooksByAuthorParamRow> getBooksByAuthorParam(Integer birthYear) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksByAuthorParam(conn, birthYear);
        }
    }

    public List<Book> getAllBookFields() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAllBookFields(conn);
        }
    }

    public List<Queries.GetBooksNotByAuthorRow> getBooksNotByAuthor(String name) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksNotByAuthor(conn, name);
        }
    }

    public List<Queries.GetBooksWithRecentSalesRow> getBooksWithRecentSales(String orderedAt) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksWithRecentSales(conn, orderedAt);
        }
    }

    public List<Queries.GetBookWithAuthorNameRow> getBookWithAuthorName() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookWithAuthorName(conn);
        }
    }

    public List<Queries.GetAuthorStatsRow> getAuthorStats() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAuthorStats(conn);
        }
    }

    public Optional<Product> getProduct(String id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getProduct(conn, id);
        }
    }

    public List<Queries.ListActiveProductsRow> listActiveProducts(int active) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listActiveProducts(conn, active);
        }
    }

    public List<Queries.GetAuthorsWithNullBioRow> getAuthorsWithNullBio() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAuthorsWithNullBio(conn);
        }
    }

    public List<Author> getAuthorsWithBio() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getAuthorsWithBio(conn);
        }
    }

    public List<Queries.GetBooksPublishedBetweenRow> getBooksPublishedBetween(String publishedAt, String publishedAt2) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksPublishedBetween(conn, publishedAt, publishedAt2);
        }
    }

    public List<Queries.GetDistinctGenresRow> getDistinctGenres() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getDistinctGenres(conn);
        }
    }

    public List<Queries.GetBooksWithSalesCountRow> getBooksWithSalesCount() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBooksWithSalesCount(conn);
        }
    }

    public Optional<Queries.CountSaleItemsRow> countSaleItems(int saleId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.countSaleItems(conn, saleId);
        }
    }

    public void updateAuthorBio(String bio, int id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.updateAuthorBio(conn, bio, id);
        }
    }

    public void deleteAuthor(int id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.deleteAuthor(conn, id);
        }
    }

    public void insertProduct(String id, String sku, String name, int active, Float weightKg, Float rating, String metadata, byte[] thumbnail, int stockCount) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.insertProduct(conn, id, sku, name, active, weightKg, rating, metadata, thumbnail, stockCount);
        }
    }

    public void upsertProduct(String id, String sku, String name, int active, String metadata, int stockCount) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.upsertProduct(conn, id, sku, name, active, metadata, stockCount);
        }
    }

    public Optional<Queries.GetSaleItemQuantityAggregatesRow> getSaleItemQuantityAggregates() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getSaleItemQuantityAggregates(conn);
        }
    }

    public Optional<Queries.GetBookPriceAggregatesRow> getBookPriceAggregates() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getBookPriceAggregates(conn);
        }
    }
}
