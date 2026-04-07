package com.example.db.queries;

import com.example.db.models.Priority;
import com.example.db.models.Status;
import com.example.db.models.Task;
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

    public Optional<Task> createTask(String title, Priority priority, Status status, String description) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.createTask(conn, title, priority, status, description);
        }
    }

    public Optional<Task> getTask(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getTask(conn, id);
        }
    }

    public List<Queries.ListTasksByPriorityRow> listTasksByPriority(Priority priority) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listTasksByPriority(conn, priority);
        }
    }

    public List<Queries.ListTasksByStatusRow> listTasksByStatus(Status status) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listTasksByStatus(conn, status);
        }
    }

    public Optional<Task> updateTaskStatus(Status status, long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.updateTaskStatus(conn, status, id);
        }
    }

    public List<Queries.ListTasksByPriorityOrAllRow> listTasksByPriorityOrAll(Priority priority) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.listTasksByPriorityOrAll(conn, priority);
        }
    }

    public List<Queries.CountByStatusRow> countByStatus() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.countByStatus(conn);
        }
    }

    public void deleteTask(long id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.deleteTask(conn, id);
        }
    }
}
