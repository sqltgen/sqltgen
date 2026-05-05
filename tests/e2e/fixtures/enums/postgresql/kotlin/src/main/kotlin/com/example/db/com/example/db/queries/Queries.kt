package com.example.db.queries

import java.sql.Connection
import com.example.db.models.Priority
import com.example.db.models.Status
import com.example.db.models.Task

object Queries {

    private val SQL_CREATE_TASK = """
        INSERT INTO task (title, priority, status, description)
        VALUES (?, ?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createTask(conn: Connection, title: String, priority: Priority, status: Status, description: String?): Task? {
        conn.prepareStatement(SQL_CREATE_TASK).use { ps ->
            ps.setString(1, title)
            ps.setObject(2, priority.value, java.sql.Types.OTHER)
            ps.setObject(3, status.value, java.sql.Types.OTHER)
            ps.setObject(4, description)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), (rs.getArray(6).array as Array<*>).map { Priority.fromValue(it as String) }.toList())
            }
        }
    }

    data class GetTaskRow(
        val id: Long,
        val title: String,
        val priority: Priority,
        val status: Status,
        val description: String?
    )

    private val SQL_GET_TASK = """
        SELECT id, title, priority, status, description
        FROM task
        WHERE id = ?;
    """.trimIndent()
    fun getTask(conn: Connection, id: Long): GetTaskRow? {
        conn.prepareStatement(SQL_GET_TASK).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return GetTaskRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5))
            }
        }
    }

    data class ListTasksByPriorityRow(
        val id: Long,
        val title: String,
        val priority: Priority,
        val status: Status
    )

    private val SQL_LIST_TASKS_BY_PRIORITY = """
        SELECT id, title, priority, status
        FROM task
        WHERE priority = ?
        ORDER BY id;
    """.trimIndent()
    fun listTasksByPriority(conn: Connection, priority: Priority): List<ListTasksByPriorityRow> {
        conn.prepareStatement(SQL_LIST_TASKS_BY_PRIORITY).use { ps ->
            ps.setObject(1, priority.value, java.sql.Types.OTHER)
            val rows = mutableListOf<ListTasksByPriorityRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListTasksByPriorityRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))))
            }
            return rows
        }
    }

    data class ListTasksByStatusRow(
        val id: Long,
        val title: String,
        val priority: Priority,
        val status: Status
    )

    private val SQL_LIST_TASKS_BY_STATUS = """
        SELECT id, title, priority, status
        FROM task
        WHERE status = ?
        ORDER BY id;
    """.trimIndent()
    fun listTasksByStatus(conn: Connection, status: Status): List<ListTasksByStatusRow> {
        conn.prepareStatement(SQL_LIST_TASKS_BY_STATUS).use { ps ->
            ps.setObject(1, status.value, java.sql.Types.OTHER)
            val rows = mutableListOf<ListTasksByStatusRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListTasksByStatusRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))))
            }
            return rows
        }
    }

    private val SQL_UPDATE_TASK_STATUS = """
        UPDATE task SET status = ? WHERE id = ?
        RETURNING *;
    """.trimIndent()
    fun updateTaskStatus(conn: Connection, status: Status, id: Long): Task? {
        conn.prepareStatement(SQL_UPDATE_TASK_STATUS).use { ps ->
            ps.setObject(1, status.value, java.sql.Types.OTHER)
            ps.setLong(2, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), (rs.getArray(6).array as Array<*>).map { Priority.fromValue(it as String) }.toList())
            }
        }
    }

    data class ListTasksByPriorityOrAllRow(
        val id: Long,
        val title: String,
        val priority: Priority,
        val status: Status
    )

    private val SQL_LIST_TASKS_BY_PRIORITY_OR_ALL = """
        SELECT id, title, priority, status
        FROM task
        WHERE (?::priority IS NULL OR priority = ?::priority)
        ORDER BY id;
    """.trimIndent()
    fun listTasksByPriorityOrAll(conn: Connection, priority: Priority?): List<ListTasksByPriorityOrAllRow> {
        conn.prepareStatement(SQL_LIST_TASKS_BY_PRIORITY_OR_ALL).use { ps ->
            ps.setObject(1, priority?.value, java.sql.Types.OTHER)
            ps.setObject(2, priority?.value, java.sql.Types.OTHER)
            val rows = mutableListOf<ListTasksByPriorityOrAllRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListTasksByPriorityOrAllRow(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4))))
            }
            return rows
        }
    }

    data class CountByStatusRow(
        val status: Status,
        val taskCount: Long
    )

    private val SQL_COUNT_BY_STATUS = """
        SELECT status, COUNT(*) AS task_count
        FROM task
        GROUP BY status
        ORDER BY status;
    """.trimIndent()
    fun countByStatus(conn: Connection): List<CountByStatusRow> {
        conn.prepareStatement(SQL_COUNT_BY_STATUS).use { ps ->
            val rows = mutableListOf<CountByStatusRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(CountByStatusRow(Status.fromValue(rs.getString(1)), rs.getLong(2)))
            }
            return rows
        }
    }

    private val SQL_CREATE_TASK_WITH_TAGS = """
        INSERT INTO task (title, priority, status, description, tags)
        VALUES (?, ?, ?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createTaskWithTags(conn: Connection, title: String, priority: Priority, status: Status, description: String?, tags: List<Priority>): Task? {
        conn.prepareStatement(SQL_CREATE_TASK_WITH_TAGS).use { ps ->
            ps.setString(1, title)
            ps.setObject(2, priority.value, java.sql.Types.OTHER)
            ps.setObject(3, status.value, java.sql.Types.OTHER)
            ps.setObject(4, description)
            ps.setArray(5, conn.createArrayOf("priority", tags.map { it.value }.toTypedArray()))
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), (rs.getArray(6).array as Array<*>).map { Priority.fromValue(it as String) }.toList())
            }
        }
    }

    data class GetTaskTagsRow(
        val id: Long,
        val title: String,
        val tags: List<Priority>
    )

    private val SQL_GET_TASK_TAGS = """
        SELECT id, title, tags
        FROM task
        WHERE id = ?;
    """.trimIndent()
    fun getTaskTags(conn: Connection, id: Long): GetTaskTagsRow? {
        conn.prepareStatement(SQL_GET_TASK_TAGS).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return GetTaskTagsRow(rs.getLong(1), rs.getString(2), (rs.getArray(3).array as Array<*>).map { Priority.fromValue(it as String) }.toList())
            }
        }
    }

    private val SQL_UPDATE_TASK_TAGS = """
        UPDATE task SET tags = ? WHERE id = ?
        RETURNING *;
    """.trimIndent()
    fun updateTaskTags(conn: Connection, tags: List<Priority>, id: Long): Task? {
        conn.prepareStatement(SQL_UPDATE_TASK_TAGS).use { ps ->
            ps.setArray(1, conn.createArrayOf("priority", tags.map { it.value }.toTypedArray()))
            ps.setLong(2, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Task(rs.getLong(1), rs.getString(2), Priority.fromValue(rs.getString(3)), Status.fromValue(rs.getString(4)), rs.getString(5), (rs.getArray(6).array as Array<*>).map { Priority.fromValue(it as String) }.toList())
            }
        }
    }

    private val SQL_DELETE_TASK = """
        DELETE FROM task WHERE id = ?;
    """.trimIndent()
    fun deleteTask(conn: Connection, id: Long): Unit {
        conn.prepareStatement(SQL_DELETE_TASK).use { ps ->
            ps.setLong(1, id)
            ps.executeUpdate()
        }
    }

    private fun getNullableBoolean(rs: java.sql.ResultSet, col: Int): Boolean? {
        val v = rs.getBoolean(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableShort(rs: java.sql.ResultSet, col: Int): Short? {
        val v = rs.getShort(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableInt(rs: java.sql.ResultSet, col: Int): Int? {
        val v = rs.getInt(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableLong(rs: java.sql.ResultSet, col: Int): Long? {
        val v = rs.getLong(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableFloat(rs: java.sql.ResultSet, col: Int): Float? {
        val v = rs.getFloat(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableDouble(rs: java.sql.ResultSet, col: Int): Double? {
        val v = rs.getDouble(col)
        return if (rs.wasNull()) null else v
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> jdbcArrayToList(arr: java.sql.Array): List<T> =
        (arr.array as Array<T>).toList()
}
