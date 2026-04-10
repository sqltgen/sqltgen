package db.queries;

import db.models.Internal_AuditLog;
import db.models.Users;
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

    public Optional<Users> getUser(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getUser(conn, id);
        }
    }

    public List<Internal_AuditLog> listAuditLogs() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listAuditLogs(conn);
        }
    }

    public void createAuditLog(long userId, String action) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.createAuditLog(conn, userId, action);
        }
    }
}
