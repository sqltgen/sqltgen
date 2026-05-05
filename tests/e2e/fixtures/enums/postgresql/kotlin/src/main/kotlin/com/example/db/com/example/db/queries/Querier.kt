package com.example.db.queries

import com.example.db.models.Priority
import com.example.db.models.Status
import com.example.db.models.Task
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun createTask(title: String, priority: Priority, status: Status, description: String?): Task? =
        dataSource.connection.use { conn -> Queries.createTask(conn, title, priority, status, description) }

    fun getTask(id: Long): Queries.GetTaskRow? =
        dataSource.connection.use { conn -> Queries.getTask(conn, id) }

    fun listTasksByPriority(priority: Priority): List<Queries.ListTasksByPriorityRow> =
        dataSource.connection.use { conn -> Queries.listTasksByPriority(conn, priority) }

    fun listTasksByStatus(status: Status): List<Queries.ListTasksByStatusRow> =
        dataSource.connection.use { conn -> Queries.listTasksByStatus(conn, status) }

    fun updateTaskStatus(status: Status, id: Long): Task? =
        dataSource.connection.use { conn -> Queries.updateTaskStatus(conn, status, id) }

    fun listTasksByPriorityOrAll(priority: Priority?): List<Queries.ListTasksByPriorityOrAllRow> =
        dataSource.connection.use { conn -> Queries.listTasksByPriorityOrAll(conn, priority) }

    fun countByStatus(): List<Queries.CountByStatusRow> =
        dataSource.connection.use { conn -> Queries.countByStatus(conn) }

    fun createTaskWithTags(title: String, priority: Priority, status: Status, description: String?, tags: List<Priority>): Task? =
        dataSource.connection.use { conn -> Queries.createTaskWithTags(conn, title, priority, status, description, tags) }

    fun getTaskTags(id: Long): Queries.GetTaskTagsRow? =
        dataSource.connection.use { conn -> Queries.getTaskTags(conn, id) }

    fun updateTaskTags(tags: List<Priority>, id: Long): Task? =
        dataSource.connection.use { conn -> Queries.updateTaskTags(conn, tags, id) }

    fun deleteTask(id: Long): Unit =
        dataSource.connection.use { conn -> Queries.deleteTask(conn, id) }
}
