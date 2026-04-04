package db.queries;

import db.models.BookSummaries;
import db.models.SciFiBooks;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;

    public Querier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<BookSummaries> listBookSummaries() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBookSummaries(conn);
        }
    }

    public List<BookSummaries> listBookSummariesByGenre(String genre) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listBookSummariesByGenre(conn, genre);
        }
    }

    public List<SciFiBooks> listSciFiBooks() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listSciFiBooks(conn);
        }
    }
}
