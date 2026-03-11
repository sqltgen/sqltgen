package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    private static final String HOST = "localhost";
    private static final int    PORT = 5433;
    private static final String USER = "sqltgen";
    private static final String PASS = "sqltgen";

    public static void main(String[] args) throws Exception {
        String migrationsDir = System.getenv("MIGRATIONS_DIR");
        if (migrationsDir == null) {
            String url = System.getenv().getOrDefault(
                    "DATABASE_URL", "jdbc:postgresql://" + HOST + ":" + PORT + "/sqltgen");
            Demo.run(url);
            return;
        }

        String db       = "sqltgen_" + Long.toHexString(System.nanoTime() & 0xFFFFFFFFL);
        String adminUrl = "jdbc:postgresql://" + HOST + ":" + PORT + "/postgres";
        String dbUrl    = "jdbc:postgresql://" + HOST + ":" + PORT + "/" + db;

        createDatabase(adminUrl, db);
        try {
            applyMigrations(dbUrl, migrationsDir);
            Demo.run(dbUrl);
        } finally {
            dropDatabase(adminUrl, db);
        }
    }

    private static void createDatabase(String adminUrl, String db) throws Exception {
        try (Connection c = DriverManager.getConnection(adminUrl, USER, PASS);
             Statement  s = c.createStatement()) {
            s.execute("CREATE DATABASE \"" + db + "\"");
        }
    }

    private static void applyMigrations(String url, String dir) throws Exception {
        List<Path> files;
        try (Stream<Path> stream = Files.list(Path.of(dir))) {
            files = stream.filter(p -> p.toString().endsWith(".sql")).sorted().toList();
        }
        try (Connection c = DriverManager.getConnection(url, USER, PASS);
             Statement  s = c.createStatement()) {
            for (Path f : files) {
                s.execute(Files.readString(f));
            }
        }
    }

    private static void dropDatabase(String adminUrl, String db) {
        try (Connection c = DriverManager.getConnection(adminUrl, USER, PASS);
             Statement  s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS \"" + db + "\"");
        } catch (Exception e) {
            System.err.println("[pg] warning: could not drop database " + db + ": " + e.getMessage());
        }
    }
}
