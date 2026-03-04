package com.example;

import com.example.db.pg.Queries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class PostgresqlDemo {

    public static void run() throws Exception {
        // H2 in-memory database stands in for PostgreSQL in this demo.
        String url = "jdbc:h2:mem:pg_demo;DB_CLOSE_DELAY=-1";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            createSchema(conn);

            Queries.createUser(conn, "Alice", "alice@example.com", "Loves hiking");
            Queries.createUser(conn, "Bob",   "bob@example.com",   null);
            Queries.createUser(conn, "Carol", "carol@example.com", "Software engineer");
            System.out.println("[pg] Inserted 3 users.");

            var all = Queries.listUsers(conn);
            System.out.println("[pg] listUsers: " + all.size() + " row(s)");
            all.forEach(u -> System.out.println("  " + u));

            Queries.getUser(conn, 1L).ifPresent(
                u -> System.out.println("[pg] getUser(1): " + u));

            Queries.deleteUser(conn, 2L);
            System.out.println("[pg] Deleted user 2.");

            Queries.createPost(conn, 1L, "Hello World",     "My first post.");
            Queries.createPost(conn, 1L, "Hiking the Alps", "What a trip!");
            Queries.createPost(conn, 3L, "Rust vs Java",    "Both are great.");
            System.out.println("[pg] Inserted 3 posts.");

            System.out.println("[pg] listPostsByUser(1):");
            Queries.listPostsByUser(conn, 1L)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[pg] listPostsWithAuthor:");
            Queries.listPostsWithAuthor(conn)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[pg] getActiveAuthors:");
            Queries.getActiveAuthors(conn)
                .forEach(a -> System.out.println("  " + a));
        }
    }

    private static void createSchema(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE users (
                    id    BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name  VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    bio   VARCHAR(1024)
                )""");
            st.execute("""
                CREATE TABLE posts (
                    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT       NOT NULL,
                    title   VARCHAR(255) NOT NULL,
                    body    VARCHAR(4096),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )""");
        }
    }
}
