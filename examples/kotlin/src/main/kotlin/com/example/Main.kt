package com.example

import com.example.db.Queries
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

fun main() {
    // H2 in-memory database — no server required for this demo.
    // The generated Queries object works with any JDBC-compatible database.
    val url = "jdbc:h2:mem:demo;DB_CLOSE_DELAY=-1"

    DriverManager.getConnection(url, "sa", "").use { conn ->
        createSchema(conn)

        // ── Users ────────────────────────────────────────────────────────────
        Queries.createUser(conn, "Alice", "alice@example.com", "Loves hiking")
        Queries.createUser(conn, "Bob",   "bob@example.com",   null)
        Queries.createUser(conn, "Carol", "carol@example.com", "Software engineer")
        println("Inserted 3 users.")

        val all = Queries.listUsers(conn)
        println("\nAll users:")
        all.forEach { println("  $it") }

        val found = Queries.getUser(conn, 1L)
        if (found != null) println("\nFetched user 1: $found")
        else println("\nUser 1 not found.")

        Queries.deleteUser(conn, 2L)
        println("\nDeleted user 2.")

        // ── Posts ────────────────────────────────────────────────────────────
        Queries.createPost(conn, 1L, "Hello World",     "My first post.")
        Queries.createPost(conn, 1L, "Hiking the Alps", "What a trip!")
        Queries.createPost(conn, 3L, "Rust vs Java",    "Both are great.")
        println("\nInserted 3 posts.")

        // Single-table query — returns Posts data class
        println("\nPosts by Alice (user 1):")
        Queries.listPostsByUser(conn, 1L).forEach { println("  $it") }

        // JOIN query — returns ListPostsWithAuthorRow
        println("\nAll posts with author:")
        Queries.listPostsWithAuthor(conn).forEach { println("  $it") }

        // Derived-table query — subquery computes per-user post count
        println("\nUsers with post count:")
        Queries.listUsersWithPostCount(conn).forEach { println("  $it") }
    }
}

private fun createSchema(conn: Connection) {
    // H2-compatible DDL that mirrors schema.sql semantics.
    // On a real PostgreSQL server you would run schema.sql directly.
    conn.createStatement().use { st: Statement ->
        st.execute("""
            CREATE TABLE users (
                id    BIGINT AUTO_INCREMENT PRIMARY KEY,
                name  VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL
            )
        """.trimIndent())
        st.execute("ALTER TABLE users ADD COLUMN bio VARCHAR(1024)")
        st.execute("""
            CREATE TABLE posts (
                id      BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT       NOT NULL,
                title   VARCHAR(255) NOT NULL,
                body    VARCHAR(4096),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """.trimIndent())
    }
}
