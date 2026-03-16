package db;

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

    public Optional<Event> getEvent(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getEvent(conn, id);
        }
    }

    public List<Event> listEvents() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listEvents(conn);
        }
    }

    public void insertEvent(String name, String payload, String meta, java.util.UUID docId, java.time.LocalDateTime createdAt, java.time.OffsetDateTime scheduledAt, java.time.LocalDate eventDate, java.time.LocalTime eventTime) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.insertEvent(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime);
        }
    }

    public void updatePayload(String payload, String meta, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.updatePayload(conn, payload, meta, id);
        }
    }

    public Optional<Queries.FindByDateRow> findByDate(java.time.LocalDate eventDate) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.findByDate(conn, eventDate);
        }
    }

    public Optional<Queries.FindByUuidRow> findByUuid(java.util.UUID docId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.findByUuid(conn, docId);
        }
    }
}
