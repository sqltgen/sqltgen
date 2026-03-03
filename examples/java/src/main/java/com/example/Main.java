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

            // Insert
            Queries.createUser(conn, "Alice", "alice@example.com", "Loves hiking");
            Queries.createUser(conn, "Bob",   "bob@example.com",   null);
            Queries.createUser(conn, "Carol", "carol@example.com", "Software engineer");
            System.out.println("Inserted 3 users.");

            // List all
            List<Users> all = Queries.listUsers(conn);
            System.out.println("\nAll users:");
            all.forEach(u -> System.out.println("  " + u));

            // Fetch one by id
            Optional<Users> found = Queries.getUser(conn, 1L);
            found.ifPresentOrElse(
                u  -> System.out.println("\nFetched user 1: " + u),
                () -> System.out.println("\nUser 1 not found.")
            );

            // Delete
            Queries.deleteUser(conn, 2L);
            System.out.println("\nDeleted user 2.");

            // List again to confirm
            System.out.println("\nRemaining users:");
            Queries.listUsers(conn).forEach(u -> System.out.println("  " + u));
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
        }
    }
}
