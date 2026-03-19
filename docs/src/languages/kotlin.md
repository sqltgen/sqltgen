# Kotlin

sqltgen generates JDBC-based Kotlin code using the standard `java.sql` package.
Generated code uses Kotlin idioms: data classes, nullable types, and Kotlin objects.

## Configuration

```json
"kotlin": {
  "out": "src/main/kotlin",
  "package": "com.example.db"
}
```

| Field | Description |
|---|---|
| `out` | Output root directory. |
| `package` | Kotlin package name. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
src/main/kotlin/com/example/db/
  Author.kt         — data class for the author table
  Book.kt           — data class for the book table
  Queries.kt        — Kotlin object with query functions
  Querier.kt        — DataSource-backed wrapper object
```

### Model data classes

```kotlin
// Author.kt
package com.example.db

data class Author(
    val id: Long,
    val name: String,
    val bio: String?,      // nullable → T?
    val birthYear: Int?    // nullable → T?
)
```

- Non-null columns use non-nullable Kotlin types (`Long`, `Int`, `Boolean`, etc.).
- Nullable columns use Kotlin nullable types (`Long?`, `Int?`, `String?`, etc.).
- `snake_case` SQL names → `camelCase` Kotlin property names.

### Query functions

```kotlin
// Queries.kt
package com.example.db

import java.sql.Connection

object Queries {

    fun getAuthor(conn: Connection, id: Long): Author? { … }

    fun listAuthors(conn: Connection): List<Author> { … }

    fun createAuthor(conn: Connection, name: String, bio: String?,
                     birthYear: Int?): Author? { … }

    fun deleteAuthor(conn: Connection, id: Long): Unit { … }

    fun countAuthors(conn: Connection): Long { … }  // :execrows
}
```

### Querier wrapper

```kotlin
// Querier.kt
package com.example.db

import javax.sql.DataSource

class Querier(private val ds: DataSource) {

    fun getAuthor(id: Long): Author? =
        ds.connection.use { Queries.getAuthor(it, id) }

    fun listAuthors(): List<Author> =
        ds.connection.use { Queries.listAuthors(it) }
    // …
}
```

## Wiring up

### Gradle dependency

```kotlin
// build.gradle.kts — PostgreSQL driver
dependencies {
    implementation("org.postgresql:postgresql:42.7.3")
    // SQLite:
    // implementation("org.xerial:sqlite-jdbc:3.47.0.0")
    // MySQL:
    // implementation("com.mysql:mysql-connector-j:9.1.0")
}
```

### Using a plain Connection

```kotlin
import java.sql.DriverManager
import com.example.db.Queries

val conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "pass")

val author = Queries.getAuthor(conn, 1L)
val all    = Queries.listAuthors(conn)
```

### Using the Querier wrapper

```kotlin
import com.zaxxer.hikari.HikariDataSource
import com.example.db.Querier

val ds = HikariDataSource().apply {
    jdbcUrl  = "jdbc:postgresql://localhost:5432/mydb"
    username = "user"
    password = "pass"
}

val q = Querier(ds)
val author = q.getAuthor(1L)
```

## Inline row types

```kotlin
object Queries {

    data class ListBooksWithAuthorRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal,
        val authorName: String,
        val authorBio: String?
    )

    fun listBooksWithAuthor(conn: Connection): List<ListBooksWithAuthorRow> { … }
}
```

## Naming conventions

| SQL | Kotlin |
|---|---|
| `get_author` | `getAuthor` |
| `birth_year` column | `birthYear` property |
| `Author` table | `Author` data class |
