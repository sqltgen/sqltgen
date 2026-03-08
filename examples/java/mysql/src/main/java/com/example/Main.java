package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    private static final String HOST      = "127.0.0.1";
    private static final int    PORT      = 3307;
    private static final String USER      = "sqltgen";
    private static final String PASS      = "sqltgen";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASS = "sqltgen_root";
    private static final String OPTS      = "?allowPublicKeyRetrieval=true&useSSL=false";

    public static void main(String[] args) throws Exception {
        String migrationsDir = System.getenv("MIGRATIONS_DIR");
        if (migrationsDir == null) {
            String url = System.getenv().getOrDefault(
                    "DATABASE_URL",
                    "jdbc:mysql://" + HOST + ":" + PORT + "/sqltgen" + OPTS);
            Demo.run(url);
            return;
        }

        String db       = "sqltgen_" + Long.toHexString(System.nanoTime() & 0xFFFFFFFFL);
        String rootUrl  = "jdbc:mysql://" + HOST + ":" + PORT + "/" + OPTS;
        String dbUrl    = "jdbc:mysql://" + HOST + ":" + PORT + "/" + db + OPTS;

        createDatabase(rootUrl, db);
        try {
            applyMigrations(dbUrl, migrationsDir);
            Demo.run(dbUrl);
        } finally {
            dropDatabase(rootUrl, db);
        }
    }

    private static void createDatabase(String rootUrl, String db) throws Exception {
        try (Connection c = DriverManager.getConnection(rootUrl, ROOT_USER, ROOT_PASS);
             Statement  s = c.createStatement()) {
            s.execute("CREATE DATABASE `" + db + "`");
            s.execute("GRANT ALL ON `" + db + "`.* TO '" + USER + "'@'%'");
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
                for (String stmt : Files.readString(f).split(";")) {
                    String sql = stmt.strip();
                    if (!sql.isEmpty()) s.execute(sql);
                }
            }
        }
    }

    private static void dropDatabase(String rootUrl, String db) {
        try (Connection c = DriverManager.getConnection(rootUrl, ROOT_USER, ROOT_PASS);
             Statement  s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS `" + db + "`");
        } catch (Exception e) {
            System.err.println("[mysql] warning: could not drop database " + db + ": " + e.getMessage());
        }
    }
}
