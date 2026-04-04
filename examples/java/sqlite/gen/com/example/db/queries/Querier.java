package com.example.db.queries;

import com.example.db.models.Author;
import com.example.db.models.Book;
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
}
