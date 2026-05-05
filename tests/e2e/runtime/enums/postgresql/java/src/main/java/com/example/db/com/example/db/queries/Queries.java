package com.example.db.queries;

import com.example.db.models.Priority;
import com.example.db.models.Status;
import com.example.db.models.Task;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Queries {
    private Queries() {}

    private static final String SQL_CREATE_TASK = """
            INSERT INTO task (title, priority, status, description)
            VALUES (?, ?, ?, ?)
            RETURNING *;
            """;
    public static Optional<Task> createTask(Connection conn, String title, Priority priority, Status status, String description) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_TASK)) {
            ps.setString(1, title);
            ps.setObject(2, priority.getValue(), java.sql.Types.OTHER);
            ps.setObject(3, status.getValue(), java.sql.Types.OTHER);
            ps.setObject(4, description);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), java.util.Arrays.stream((Object[]) rs.getArray(6).getArray()).map(it -> Priority.fromValue((String) it)).collect(java.util.stream.Collectors.toList())));
            }
        }
    }

    public record GetTaskRow(
        long id,
        String title,
        Priority priority,
        Status status,
        String description
    ) {}

    private static final String SQL_GET_TASK = """
            SELECT id, title, priority, status, description
            FROM task
            WHERE id = ?;
            """;
    public static Optional<GetTaskRow> getTask(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_TASK)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new GetTaskRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5)));
            }
        }
    }

    public record ListTasksByPriorityRow(
        long id,
        String title,
        Priority priority,
        Status status
    ) {}

    private static final String SQL_LIST_TASKS_BY_PRIORITY = """
            SELECT id, title, priority, status
            FROM task
            WHERE priority = ?
            ORDER BY id;
            """;
    public static List<ListTasksByPriorityRow> listTasksByPriority(Connection conn, Priority priority) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_TASKS_BY_PRIORITY)) {
            ps.setObject(1, priority.getValue(), java.sql.Types.OTHER);
            List<ListTasksByPriorityRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListTasksByPriorityRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))));
            }
            return rows;
        }
    }

    public record ListTasksByStatusRow(
        long id,
        String title,
        Priority priority,
        Status status
    ) {}

    private static final String SQL_LIST_TASKS_BY_STATUS = """
            SELECT id, title, priority, status
            FROM task
            WHERE status = ?
            ORDER BY id;
            """;
    public static List<ListTasksByStatusRow> listTasksByStatus(Connection conn, Status status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_TASKS_BY_STATUS)) {
            ps.setObject(1, status.getValue(), java.sql.Types.OTHER);
            List<ListTasksByStatusRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListTasksByStatusRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))));
            }
            return rows;
        }
    }

    private static final String SQL_UPDATE_TASK_STATUS = """
            UPDATE task SET status = ? WHERE id = ?
            RETURNING *;
            """;
    public static Optional<Task> updateTaskStatus(Connection conn, Status status, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_TASK_STATUS)) {
            ps.setObject(1, status.getValue(), java.sql.Types.OTHER);
            ps.setLong(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), java.util.Arrays.stream((Object[]) rs.getArray(6).getArray()).map(it -> Priority.fromValue((String) it)).collect(java.util.stream.Collectors.toList())));
            }
        }
    }

    public record ListTasksByPriorityOrAllRow(
        long id,
        String title,
        Priority priority,
        Status status
    ) {}

    private static final String SQL_LIST_TASKS_BY_PRIORITY_OR_ALL = """
            SELECT id, title, priority, status
            FROM task
            WHERE (?::priority IS NULL OR priority = ?::priority)
            ORDER BY id;
            """;
    public static List<ListTasksByPriorityOrAllRow> listTasksByPriorityOrAll(Connection conn, Priority priority) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LIST_TASKS_BY_PRIORITY_OR_ALL)) {
            ps.setObject(1, priority != null ? priority.getValue() : null, java.sql.Types.OTHER);
            ps.setObject(2, priority != null ? priority.getValue() : null, java.sql.Types.OTHER);
            List<ListTasksByPriorityOrAllRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new ListTasksByPriorityOrAllRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))));
            }
            return rows;
        }
    }

    public record CountByStatusRow(
        Status status,
        long taskCount
    ) {}

    private static final String SQL_COUNT_BY_STATUS = """
            SELECT status, COUNT(*) AS task_count
            FROM task
            GROUP BY status
            ORDER BY status;
            """;
    public static List<CountByStatusRow> countByStatus(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_COUNT_BY_STATUS)) {
            List<CountByStatusRow> rows = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) rows.add(new CountByStatusRow(Status.fromValue(rs.getString(1)), rs.getLong(2)));
            }
            return rows;
        }
    }

    private static final String SQL_CREATE_TASK_WITH_TAGS = """
            INSERT INTO task (title, priority, status, description, tags)
            VALUES (?, ?, ?, ?, ?)
            RETURNING *;
            """;
    public static Optional<Task> createTaskWithTags(Connection conn, String title, Priority priority, Status status, String description, java.util.List<Priority> tags) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_CREATE_TASK_WITH_TAGS)) {
            ps.setString(1, title);
            ps.setObject(2, priority.getValue(), java.sql.Types.OTHER);
            ps.setObject(3, status.getValue(), java.sql.Types.OTHER);
            ps.setObject(4, description);
            ps.setArray(5, conn.createArrayOf("priority", tags.stream().map(e -> e.getValue()).toArray(String[]::new)));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), java.util.Arrays.stream((Object[]) rs.getArray(6).getArray()).map(it -> Priority.fromValue((String) it)).collect(java.util.stream.Collectors.toList())));
            }
        }
    }

    public record GetTaskTagsRow(
        long id,
        String title,
        java.util.List<Priority> tags
    ) {}

    private static final String SQL_GET_TASK_TAGS = """
            SELECT id, title, tags
            FROM task
            WHERE id = ?;
            """;
    public static Optional<GetTaskTagsRow> getTaskTags(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_GET_TASK_TAGS)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new GetTaskTagsRow(rs.getLong(1), rs.getString(2), java.util.Arrays.stream((Object[]) rs.getArray(3).getArray()).map(it -> Priority.fromValue((String) it)).collect(java.util.stream.Collectors.toList())));
            }
        }
    }

    private static final String SQL_UPDATE_TASK_TAGS = """
            UPDATE task SET tags = ? WHERE id = ?
            RETURNING *;
            """;
    public static Optional<Task> updateTaskTags(Connection conn, java.util.List<Priority> tags, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_TASK_TAGS)) {
            ps.setArray(1, conn.createArrayOf("priority", tags.stream().map(e -> e.getValue()).toArray(String[]::new)));
            ps.setLong(2, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), java.util.Arrays.stream((Object[]) rs.getArray(6).getArray()).map(it -> Priority.fromValue((String) it)).collect(java.util.stream.Collectors.toList())));
            }
        }
    }

    private static final String SQL_DELETE_TASK = """
            DELETE FROM task WHERE id = ?;
            """;
    public static void deleteTask(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_TASK)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private static Boolean getNullableBoolean(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        boolean v = rs.getBoolean(col);
        return rs.wasNull() ? null : v;
    }
    private static Short getNullableShort(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        short v = rs.getShort(col);
        return rs.wasNull() ? null : v;
    }
    private static Integer getNullableInt(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        int v = rs.getInt(col);
        return rs.wasNull() ? null : v;
    }
    private static Long getNullableLong(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
    private static Float getNullableFloat(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : v;
    }
    private static Double getNullableDouble(java.sql.ResultSet rs, int col) throws java.sql.SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }
}
