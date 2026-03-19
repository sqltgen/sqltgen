# Java

sqltgen generates JDBC-based Java code using only the standard `java.sql` package.
No framework, no reflection, no extra runtime dependencies.

## Configuration

```json
"java": {
  "out": "src/main/java",
  "package": "com.example.db"
}
```

| Field | Description |
|---|---|
| `out` | Directory under which the package path is created. |
| `package` | Java package name. Files are placed at `out/com/example/db/`. |
| `list_params` | `"native"` (default) or `"dynamic"`. See [List parameter strategies](../config.md#list-parameter-strategies). |

## What is generated

For a schema with `author` and `book` tables and a single query file, sqltgen emits:

```
src/main/java/com/example/db/
  Author.java         — Java record for the author table
  Book.java           — Java record for the book table
  Queries.java        — static query functions
  Querier.java        — DataSource-backed connection-per-call wrapper
```

When using [query grouping](../config.md#queries-field), each group produces its own
`{GroupName}Queries.java` and `{GroupName}Querier.java`.

### Model records

```java
// Author.java
package com.example.db;

public record Author(
    long id,
    String name,
    String bio,         // nullable → String (null if absent)
    Integer birthYear   // nullable → boxed Integer
) {}
```

- Non-null columns use primitive types (`long`, `int`, `boolean`, `double`, etc.).
- Nullable columns use boxed types (`Long`, `Integer`, `Boolean`, `Double`, etc.) or
  reference types (`String`, `BigDecimal`, `LocalDate`, etc.).
- SQL `snake_case` column names are converted to Java `camelCase` field names
  (`birth_year` → `birthYear`).

### Query functions

```java
// Queries.java
package com.example.db;

import java.sql.*;
import java.util.*;

public final class Queries {

    public static Optional<Author> getAuthor(Connection conn, long id)
            throws SQLException { … }

    public static List<Author> listAuthors(Connection conn)
            throws SQLException { … }

    public static Optional<Author> createAuthor(Connection conn,
            String name, String bio, Integer birthYear)
            throws SQLException { … }

    public static void deleteAuthor(Connection conn, long id)
            throws SQLException { … }

    public static long countAuthors(Connection conn)
            throws SQLException { … }  // :execrows
}
```

The SQL constant is emitted as a `private static final String SQL_…` field inside
the class. Each function prepares the statement, binds parameters, executes, and
maps the result — all in a single, self-contained method body.

### Querier wrapper

`Querier.java` wraps a `DataSource` and opens a new connection per call:

```java
// Querier.java
package com.example.db;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public final class Querier {
    private final DataSource ds;

    public Querier(DataSource ds) { this.ds = ds; }

    public Optional<Author> getAuthor(long id) throws SQLException {
        try (var conn = ds.getConnection()) {
            return Queries.getAuthor(conn, id);
        }
    }

    public List<Author> listAuthors() throws SQLException {
        try (var conn = ds.getConnection()) {
            return Queries.listAuthors(conn);
        }
    }
    // …
}
```

## Wiring up

### Maven dependency

```xml
<!-- pom.xml — PostgreSQL driver -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.3</version>
</dependency>

<!-- SQLite -->
<dependency>
    <groupId>org.xerial</groupId>
    <artifactId>sqlite-jdbc</artifactId>
    <version>3.47.0.0</version>
</dependency>

<!-- MySQL -->
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <version>9.1.0</version>
</dependency>
```

### Using a plain Connection

```java
import java.sql.Connection;
import java.sql.DriverManager;
import com.example.db.Author;
import com.example.db.Queries;

Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "pass");

Optional<Author> author = Queries.getAuthor(conn, 1L);
List<Author> all        = Queries.listAuthors(conn);
```

### Using the Querier wrapper (connection pool)

```java
import com.zaxxer.hikari.HikariDataSource;
import com.example.db.Querier;

var ds = new HikariDataSource();
ds.setJdbcUrl("jdbc:postgresql://localhost:5432/mydb");
ds.setUsername("user");
ds.setPassword("pass");

var q = new Querier(ds);
Optional<Author> author = q.getAuthor(1L);
List<Author> all        = q.listAuthors();
```

## Inline row types

When a query result does not match any single table (JOINs, partial RETURNING, etc.),
sqltgen emits an inline `record` inside `Queries.java`:

```java
public final class Queries {

    public record ListBooksWithAuthorRow(
        long id,
        String title,
        String genre,
        BigDecimal price,
        String authorName,
        String authorBio
    ) {}

    public static List<ListBooksWithAuthorRow> listBooksWithAuthor(Connection conn)
            throws SQLException { … }
}
```

## Naming conventions

| SQL | Java |
|---|---|
| `get_author` | `getAuthor` |
| `list_books_by_genre` | `listBooksByGenre` |
| `birth_year` column | `birthYear` field |
| `Author` table | `Author` record |

## Java version requirements

The generated code requires Java 16+ for records. Java 8–15 will require
modifying the record declarations to standard classes with constructors and
accessor methods.
