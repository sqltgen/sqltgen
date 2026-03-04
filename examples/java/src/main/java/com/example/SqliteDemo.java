package com.example;

import com.example.db.sqlite.Queries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class SqliteDemo {

    public static void run() throws Exception {
        String url = "jdbc:sqlite::memory:";

        try (Connection conn = DriverManager.getConnection(url)) {
            createSchema(conn);

            Queries.createUser(conn, "Carol", "carol@example.com", "Loves SQLite");
            Queries.createUser(conn, "Dave",  "dave@example.com",  null);
            System.out.println("[sqlite] Inserted 2 users.");

            var all = Queries.listUsers(conn);
            System.out.println("[sqlite] listUsers: " + all.size() + " row(s)");
            all.forEach(u -> System.out.println("  " + u));

            Queries.getUser(conn, 1).ifPresent(
                u -> System.out.println("[sqlite] getUser(1): " + u));

            Queries.createPost(conn, 1, "SQLite Post", "Written in SQLite.");
            System.out.println("[sqlite] Inserted 1 post.");

            System.out.println("[sqlite] listPostsByUser(1):");
            Queries.listPostsByUser(conn, 1)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[sqlite] listPostsWithAuthor:");
            Queries.listPostsWithAuthor(conn)
                .forEach(p -> System.out.println("  " + p));

            System.out.println("[sqlite] getActiveAuthors:");
            Queries.getActiveAuthors(conn)
                .forEach(a -> System.out.println("  " + a));
        }
    }

    private static void createSchema(Connection conn) throws Exception {
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
