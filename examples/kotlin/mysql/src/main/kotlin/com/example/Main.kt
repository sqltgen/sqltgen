package com.example

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

private const val HOST      = "127.0.0.1"
private const val PORT      = 3307
private const val USER      = "sqltgen"
private const val PASS      = "sqltgen"
private const val ROOT_USER = "root"
private const val ROOT_PASS = "sqltgen_root"
private const val OPTS      = "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true"

fun main() {
    val migrationsDir = System.getenv("MIGRATIONS_DIR")
    if (migrationsDir == null) {
        val url = System.getenv("DATABASE_URL") ?: "jdbc:mysql://$HOST:$PORT/sqltgen$OPTS"
        Demo.run(url)
        return
    }

    val db      = "sqltgen_${(System.nanoTime() and 0xFFFFFFFFL).toString(16)}"
    val rootUrl = "jdbc:mysql://$HOST:$PORT/$OPTS"
    val dbUrl   = "jdbc:mysql://$HOST:$PORT/$db$OPTS"

    DriverManager.getConnection(rootUrl, ROOT_USER, ROOT_PASS).use { c ->
        c.createStatement().use { s ->
            s.execute("CREATE DATABASE `$db`")
            s.execute("GRANT ALL ON `$db`.* TO '$USER'@'%'")
        }
    }
    try {
        val files = Files.list(Path.of(migrationsDir))
            .filter { it.toString().endsWith(".sql") }.sorted().toList()
        DriverManager.getConnection(dbUrl, USER, PASS).use { c ->
            c.createStatement().use { s ->
                files.forEach { f -> s.execute(Files.readString(f)) }
            }
        }
        Demo.run(dbUrl)
    } finally {
        try {
            DriverManager.getConnection(rootUrl, ROOT_USER, ROOT_PASS).use { c ->
                c.createStatement().use { s -> s.execute("DROP DATABASE IF EXISTS `$db`") }
            }
        } catch (e: Exception) {
            System.err.println("[mysql] warning: could not drop database $db: ${e.message}")
        }
    }
}
