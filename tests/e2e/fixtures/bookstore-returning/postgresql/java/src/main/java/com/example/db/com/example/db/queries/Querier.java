package com.example.db.queries;

import com.example.db.models.Author;
import com.example.db.models.Book;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;

    public Querier(DataSource dataSource) {
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
}
