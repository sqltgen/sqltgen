package com.example;

import com.example.db.Queries;
import com.example.db.Users;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

public class Main {

    public static void main(String[] args) throws Exception {
        // H2 in-memory database — no server required for this demo.
        // The generated Queries class works with any JDBC-compatible database.
        String url = "jdbc:h2:mem:demo;DB_CLOSE_DELAY=-1";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            createSchema(conn);

            // ── Users ────────────────────────────────────────────────────────
            Queries.createUser(conn, "Alice", "alice@example.com", "Loves hiking");
            Queries.createUser(conn, "Bob",   "bob@example.com",   null);
            Queries.createUser(conn, "Carol", "carol@example.com", "Software engineer");
            System.out.println("Inserted 3 users.");

            List<Users> all = Queries.listUsers(conn);
            System.out.println("\nAll users:");
            all.forEach(u -> System.out.println("  " + u));

            Optional<Users> found = Queries.getUser(conn, 1L);
            found.ifPresentOrElse(
                u  -> System.out.println("\nFetched user 1: " + u),
                () -> System.out.println("\nUser 1 not found.")
            );

            Queries.deleteUser(conn, 2L);
            System.out.println("\nDeleted user 2.");

            // ── Posts ────────────────────────────────────────────────────────
            Queries.createPost(conn, 1L, "Hello World",     "My first post.");
            Queries.createPost(conn, 1L, "Hiking the Alps", "What a trip!");
            Queries.createPost(conn, 3L, "Rust vs Java",    "Both are great.");
            System.out.println("\nInserted 3 posts.");

            // Single-table query — returns Posts row type
            System.out.println("\nPosts by Alice (user 1):");
            Queries.listPostsByUser(conn, 1L)
                   .forEach(p -> System.out.println("  " + p));

            // JOIN query — returns ListPostsWithAuthorRow
            System.out.println("\nAll posts with author:");
            Queries.listPostsWithAuthor(conn)
                   .forEach(p -> System.out.println("  " + p));
        }
    }

    private static void createSchema(Connection conn) throws Exception {
        // H2-compatible DDL that mirrors schema.sql semantics.
        // On a real PostgreSQL server you would run schema.sql directly.
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE users (
                    id    BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name  VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL
                )
                """);
            st.execute("ALTER TABLE users ADD COLUMN bio VARCHAR(1024)");
            st.execute("""
                CREATE TABLE posts (
                    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT       NOT NULL,
                    title   VARCHAR(255) NOT NULL,
                    body    VARCHAR(4096),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
                """);
        }
    }
}
