package com.example.db

import java.sql.Connection

object Queries {

    private const val SQL_GETUSER = "SELECT id, name, email, bio FROM users WHERE id = ?;"
    fun getUser(conn: Connection, id: Long): Users? {
        conn.prepareStatement(SQL_GETUSER).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Users(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4))
            }
        }
    }

    private const val SQL_LISTUSERS = "SELECT id, name, email, bio FROM users;"
    fun listUsers(conn: Connection): List<Users> {
        conn.prepareStatement(SQL_LISTUSERS).use { ps ->
            val rows = mutableListOf<Users>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Users(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            }
            return rows
        }
    }

    private const val SQL_CREATEUSER = "INSERT INTO users (name, email, bio) VALUES (?, ?, ?);"
    fun createUser(conn: Connection, name: String, email: String, bio: String?): Unit {
        conn.prepareStatement(SQL_CREATEUSER).use { ps ->
            ps.setString(1, name)
            ps.setString(2, email)
            ps.setString(3, bio)
            ps.executeUpdate()
        }
    }

    private const val SQL_DELETEUSER = "DELETE FROM users WHERE id = ?;"
    fun deleteUser(conn: Connection, id: Long): Unit {
        conn.prepareStatement(SQL_DELETEUSER).use { ps ->
            ps.setLong(1, id)
            ps.executeUpdate()
        }
    }

    private const val SQL_CREATEPOST = "INSERT INTO posts (user_id, title, body) VALUES (?, ?, ?);"
    fun createPost(conn: Connection, userId: Long, title: String, body: String?): Unit {
        conn.prepareStatement(SQL_CREATEPOST).use { ps ->
            ps.setLong(1, userId)
            ps.setString(2, title)
            ps.setString(3, body)
            ps.executeUpdate()
        }
    }

    data class ListPostsByUserRow(
        val id: Long,
        val title: String,
        val body: String?
    )

    private const val SQL_LISTPOSTSBYUSER = "SELECT p.id, p.title, p.body FROM posts p WHERE p.user_id = ?;"
    fun listPostsByUser(conn: Connection, userId: Long): List<ListPostsByUserRow> {
        conn.prepareStatement(SQL_LISTPOSTSBYUSER).use { ps ->
            ps.setLong(1, userId)
            val rows = mutableListOf<ListPostsByUserRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListPostsByUserRow(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }

    data class ListPostsWithAuthorRow(
        val id: Long,
        val title: String,
        val name: String,
        val email: String
    )

    private const val SQL_LISTPOSTSWITHAUTHOR = "SELECT p.id, p.title, u.name, u.email FROM posts p INNER JOIN users u ON u.id = p.user_id;"
    fun listPostsWithAuthor(conn: Connection): List<ListPostsWithAuthorRow> {
        conn.prepareStatement(SQL_LISTPOSTSWITHAUTHOR).use { ps ->
            val rows = mutableListOf<ListPostsWithAuthorRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListPostsWithAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            }
            return rows
        }
    }

    data class ListUsersWithPostCountRow(
        val name: String,
        val email: String,
        val postCount: Any?
    )

    private const val SQL_LISTUSERSWITHPOSTCOUNT = "SELECT u.name, u.email, pc.post_count FROM users u INNER JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) pc ON u.id = pc.user_id;"
    fun listUsersWithPostCount(conn: Connection): List<ListUsersWithPostCountRow> {
        conn.prepareStatement(SQL_LISTUSERSWITHPOSTCOUNT).use { ps ->
            val rows = mutableListOf<ListUsersWithPostCountRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListUsersWithPostCountRow(rs.getString(1), rs.getString(2), rs.getObject(3)))
            }
            return rows
        }
    }

    data class GetActiveAuthorsRow(
        val id: Long,
        val name: String,
        val email: String
    )

    private const val SQL_GETACTIVEAUTHORS = "WITH post_authors AS (     SELECT DISTINCT user_id FROM posts ) SELECT u.id, u.name, u.email FROM users u JOIN post_authors pa ON pa.user_id = u.id;"
    fun getActiveAuthors(conn: Connection): List<GetActiveAuthorsRow> {
        conn.prepareStatement(SQL_GETACTIVEAUTHORS).use { ps ->
            val rows = mutableListOf<GetActiveAuthorsRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetActiveAuthorsRow(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }
}
