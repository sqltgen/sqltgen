package com.example

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

private const val HOST = "localhost"
private const val PORT = 5433
private const val USER = "sqltgen"
private const val PASS = "sqltgen"

fun main() {
    val migrationsDir = System.getenv("MIGRATIONS_DIR")
    if (migrationsDir == null) {
        val url = System.getenv("DATABASE_URL") ?: "jdbc:postgresql://$HOST:$PORT/sqltgen"
        Demo.run(url)
        return
    }

    val db       = "sqltgen_${(System.nanoTime() and 0xFFFFFFFFL).toString(16)}"
    val adminUrl = "jdbc:postgresql://$HOST:$PORT/postgres"
    val dbUrl    = "jdbc:postgresql://$HOST:$PORT/$db"

    DriverManager.getConnection(adminUrl, USER, PASS).use { c ->
        c.createStatement().use { s -> s.execute("""CREATE DATABASE "$db"""") }
    }
    try {
        val files = Files.list(Path.of(migrationsDir))
            .filter { it.toString().endsWith(".sql") }.sorted().toList()
        DriverManager.getConnection(dbUrl, USER, PASS).use { c ->
            c.createStatement().use { s ->
                files.forEach { f ->
                    Files.readString(f).split(";").forEach { stmt ->
                        val sql = stmt.trim()
                        if (sql.isNotEmpty()) s.execute(sql)
                    }
                }
            }
        }
        Demo.run(dbUrl)
    } finally {
        try {
            DriverManager.getConnection(adminUrl, USER, PASS).use { c ->
                c.createStatement().use { s -> s.execute("""DROP DATABASE IF EXISTS "$db"""") }
            }
        } catch (e: Exception) {
            System.err.println("[pg] warning: could not drop database $db: ${e.message}")
        }
    }
}
