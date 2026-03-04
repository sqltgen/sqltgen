package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Main {

    public static void main(String[] args) throws Exception {
        runPostgresql();
        runSqlite();
    }

    // ── PostgreSQL-generated code demo (using H2 as a stand-in) ──────────────

    private static void runPostgresql() throws Exception {
        String url = "jdbc:h2:mem:pg_demo;DB_CLOSE_DELAY=-1";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            createPgSchema(conn);

            com.example.db.pg.Queries.createUser(conn, "Alice", "alice@example.com", "Loves hiking");
            com.example.db.pg.Queries.createUser(conn, "Bob",   "bob@example.com",   null);
            com.example.db.pg.Queries.createUser(conn, "Carol", "carol@example.com", "Software engineer");
            System.out.println("[pg] Inserted 3 users.");

            var all = com.example.db.pg.Queries.listUsers(conn);
            System.out.println("[pg] listUsers: " + all.size() + " row(s)");
            all.forEach(u -> System.out.println("  " + u));

            com.example.db.pg.Queries.getUser(conn, 1L).ifPresent(
                u -> System.out.println("[pg] getUser(1): " + u));

            com.example.db.pg.Queries.deleteUser(conn, 2L);
            System.out.println("[pg] Deleted user 2.");

            com.example.db.pg.Queries.createPost(conn, 1L, "Hello World",     "My first post.");
            com.example.db.pg.Queries.createPost(conn, 1L, "Hiking the Alps", "What a trip!");
            com.example.db.pg.Queries.createPost(conn, 3L, "Rust vs Java",    "Both are great.");
            System.out.println("[pg] Inserted 3 posts.");

            System.out.println("[pg] listPostsByUser(1):");
            com.example.db.pg.Queries.listPostsByUser(conn, 1L)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[pg] listPostsWithAuthor:");
            com.example.db.pg.Queries.listPostsWithAuthor(conn)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[pg] getActiveAuthors:");
            com.example.db.pg.Queries.getActiveAuthors(conn)
                .forEach(a -> System.out.println("  " + a));
        }
    }

    private static void createPgSchema(Connection conn) throws Exception {
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

    // ── SQLite-generated code demo ────────────────────────────────────────────

    private static void runSqlite() throws Exception {
        String url = "jdbc:sqlite::memory:";

        try (Connection conn = DriverManager.getConnection(url)) {
            createSqliteSchema(conn);

            com.example.db.sqlite.Queries.createUser(conn, "Carol", "carol@example.com", "Loves SQLite");
            com.example.db.sqlite.Queries.createUser(conn, "Dave",  "dave@example.com",  null);
            System.out.println("[sqlite] Inserted 2 users.");

            var all = com.example.db.sqlite.Queries.listUsers(conn);
            System.out.println("[sqlite] listUsers: " + all.size() + " row(s)");
            all.forEach(u -> System.out.println("  " + u));

            com.example.db.sqlite.Queries.getUser(conn, 1).ifPresent(
                u -> System.out.println("[sqlite] getUser(1): " + u));

            com.example.db.sqlite.Queries.createPost(conn, 1, "SQLite Post", "Written in SQLite.");
            System.out.println("[sqlite] Inserted 1 post.");

            System.out.println("[sqlite] listPostsByUser(1):");
            com.example.db.sqlite.Queries.listPostsByUser(conn, 1)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[sqlite] listPostsWithAuthor:");
            com.example.db.sqlite.Queries.listPostsWithAuthor(conn)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[sqlite] getActiveAuthors:");
            com.example.db.sqlite.Queries.getActiveAuthors(conn)
                .forEach(a -> System.out.println("  " + a));
        }
    }

    private static void createSqliteSchema(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE users (
                    id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    name  TEXT    NOT NULL,
                    email TEXT    NOT NULL,
                    bio   TEXT
                )""");
            st.execute("""
                CREATE TABLE posts (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    title   TEXT    NOT NULL,
                    body    TEXT
                )""");
        }
    }
}
