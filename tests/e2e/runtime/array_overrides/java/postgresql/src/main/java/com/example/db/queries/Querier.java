package com.example.db.queries;

import com.example.db.models.Record;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;

    public Querier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insertRecord(String label, java.util.List<LocalDateTime> timestamps, java.util.List<UUID> uuids) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.insertRecord(conn, label, timestamps, uuids);
        }
    }

    public Optional<Record> getRecord(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getRecord(conn, id);
        }
    }
}
