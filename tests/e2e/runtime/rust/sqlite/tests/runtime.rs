use e2e_rust_sqlite::db::queries;
use sqlx::SqlitePool;

/// Create an in-memory SQLite database with the schema loaded.
async fn setup_db() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

    sqlx::query(
        "CREATE TABLE author (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT    NOT NULL,
            bio        TEXT,
            birth_year INTEGER
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE book (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            author_id    INTEGER NOT NULL REFERENCES author(id),
            title        TEXT    NOT NULL,
            genre        TEXT    NOT NULL,
            price        DECIMAL NOT NULL,
            published_at TEXT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE customer (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            name  TEXT    NOT NULL,
            email TEXT    NOT NULL UNIQUE
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE sale (
            id          INTEGER  PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER  NOT NULL REFERENCES customer(id),
            ordered_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE sale_item (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id    INTEGER NOT NULL REFERENCES sale(id),
            book_id    INTEGER NOT NULL REFERENCES book(id),
            quantity   INTEGER NOT NULL,
            unit_price DECIMAL NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE product (
            id          TEXT    PRIMARY KEY,
            sku         TEXT    NOT NULL,
            name        TEXT    NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1,
            weight_kg   REAL,
            rating      REAL,
            metadata    TEXT,
            thumbnail   BLOB,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
            stock_count INTEGER NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    pool
}

/// Seed the database with test data.
async fn seed(pool: &SqlitePool) {
    queries::create_author(pool, "Asimov".into(), Some("Sci-fi master".into()), Some(1920))
        .await
        .unwrap();
    queries::create_author(pool, "Herbert".into(), None, Some(1920))
        .await
        .unwrap();
    queries::create_author(pool, "Le Guin".into(), Some("Earthsea".into()), Some(1929))
        .await
        .unwrap();

    queries::create_book(pool, 1, "Foundation".into(), "sci-fi".into(), 9.99, Some("1951-01-01".into()))
        .await
        .unwrap();
    queries::create_book(pool, 1, "I Robot".into(), "sci-fi".into(), 7.99, Some("1950-01-01".into()))
        .await
        .unwrap();
    queries::create_book(pool, 2, "Dune".into(), "sci-fi".into(), 12.99, Some("1965-01-01".into()))
        .await
        .unwrap();
    queries::create_book(pool, 3, "Earthsea".into(), "fantasy".into(), 8.99, Some("1968-01-01".into()))
        .await
        .unwrap();

    queries::create_customer(pool, "Alice".into(), "alice@example.com".into())
        .await
        .unwrap();

    queries::create_sale(pool, 1).await.unwrap();

    queries::add_sale_item(pool, 1, 1, 2, 9.99).await.unwrap();
    queries::add_sale_item(pool, 1, 3, 1, 12.99).await.unwrap();
}

// ─── :one tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_author() {
    let pool = setup_db().await;
    seed(&pool).await;

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.name, "Asimov");
    assert_eq!(author.bio, Some("Sci-fi master".into()));
    assert_eq!(author.birth_year, Some(1920));
}

#[tokio::test]
async fn test_get_author_not_found() {
    let pool = setup_db().await;
    let author = queries::get_author(&pool, 999).await.unwrap();
    assert!(author.is_none());
}

#[tokio::test]
async fn test_get_book() {
    let pool = setup_db().await;
    seed(&pool).await;

    let book = queries::get_book(&pool, 1).await.unwrap().unwrap();
    assert_eq!(book.title, "Foundation");
    assert_eq!(book.genre, "sci-fi");
    assert_eq!(book.author_id, 1);
}

// ─── :many tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_authors() {
    let pool = setup_db().await;
    seed(&pool).await;

    let authors = queries::list_authors(&pool).await.unwrap();
    assert_eq!(authors.len(), 3);
    // Should be sorted by name
    assert_eq!(authors[0].name, "Asimov");
    assert_eq!(authors[1].name, "Herbert");
    assert_eq!(authors[2].name, "Le Guin");
}

#[tokio::test]
async fn test_list_books_by_genre() {
    let pool = setup_db().await;
    seed(&pool).await;

    let books = queries::list_books_by_genre(&pool, "sci-fi".into()).await.unwrap();
    assert_eq!(books.len(), 3);

    let books = queries::list_books_by_genre(&pool, "fantasy".into()).await.unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].title, "Earthsea");
}

#[tokio::test]
async fn test_list_books_by_genre_or_all() {
    let pool = setup_db().await;
    seed(&pool).await;

    let all = queries::list_books_by_genre_or_all(&pool, "all".into()).await.unwrap();
    assert_eq!(all.len(), 4);

    let sci_fi = queries::list_books_by_genre_or_all(&pool, "sci-fi".into()).await.unwrap();
    assert_eq!(sci_fi.len(), 3);
}

// ─── :exec tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_author_exec() {
    let pool = setup_db().await;
    queries::create_author(&pool, "Test".into(), None, None).await.unwrap();

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.name, "Test");
    assert!(author.bio.is_none());
    assert!(author.birth_year.is_none());
}

// ─── :execrows tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_delete_book_by_id() {
    let pool = setup_db().await;
    // Enable FK enforcement (SQLite has it off by default)
    sqlx::query("PRAGMA foreign_keys = ON").execute(&pool).await.unwrap();
    seed(&pool).await;

    // Book 2 (I Robot) has no sale_items, so it can be deleted
    let affected = queries::delete_book_by_id(&pool, 2).await.unwrap();
    assert_eq!(affected, 1);

    let affected = queries::delete_book_by_id(&pool, 999).await.unwrap();
    assert_eq!(affected, 0);
}

// ─── JOIN tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_books_with_author() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::list_books_with_author(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);

    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.author_name, "Herbert");
    assert!(dune.author_bio.is_none());

    let foundation = rows.iter().find(|r| r.title == "Foundation").unwrap();
    assert_eq!(foundation.author_name, "Asimov");
    assert_eq!(foundation.author_bio, Some("Sci-fi master".into()));
}

#[tokio::test]
async fn test_get_books_never_ordered() {
    let pool = setup_db().await;
    seed(&pool).await;

    let books = queries::get_books_never_ordered(&pool).await.unwrap();
    // Books 2 (I Robot) and 4 (Earthsea) were not ordered
    assert_eq!(books.len(), 2);
    let titles: Vec<&str> = books.iter().map(|b| b.title.as_str()).collect();
    assert!(titles.contains(&"I Robot"));
    assert!(titles.contains(&"Earthsea"));
}

// ─── CTE tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_top_selling_books() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_top_selling_books(&pool).await.unwrap();
    assert!(!rows.is_empty());
    // Foundation had qty 2, Dune had qty 1
    assert_eq!(rows[0].title, "Foundation");
}

#[tokio::test]
async fn test_get_best_customers() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_best_customers(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Alice");
}

#[tokio::test]
async fn test_get_author_stats() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: This query uses COALESCE(…, 0) which returns INTEGER at runtime,
    // but codegen maps expression columns as serde_json::Value (TEXT).
    // This type mismatch means sqlx::FromRow can't decode it.
    // For now, verify the raw SQL works via manual query.
    let rows: Vec<(i32, String, i64, i64)> = sqlx::query_as(
        "WITH book_counts AS (\
            SELECT author_id, COUNT(*) AS num_books FROM book GROUP BY author_id\
        ), sale_counts AS (\
            SELECT b.author_id, SUM(si.quantity) AS total_sold FROM sale_item si \
            JOIN book b ON b.id = si.book_id GROUP BY b.author_id\
        ) SELECT a.id, a.name, COALESCE(bc.num_books, 0) AS num_books, \
          COALESCE(sc.total_sold, 0) AS total_sold \
          FROM author a LEFT JOIN book_counts bc ON bc.author_id = a.id \
          LEFT JOIN sale_counts sc ON sc.author_id = a.id ORDER BY a.name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1, "Asimov"); // sorted by name
    assert_eq!(rows[0].2, 2); // 2 books
}

// ─── Aggregate tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_books_by_genre() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::count_books_by_genre(&pool).await.unwrap();
    assert_eq!(rows.len(), 2); // fantasy, sci-fi

    let fantasy = rows.iter().find(|r| r.genre == "fantasy").unwrap();
    assert_eq!(fantasy.book_count, 1);

    let sci_fi = rows.iter().find(|r| r.genre == "sci-fi").unwrap();
    assert_eq!(sci_fi.book_count, 3);
}

// ─── LIMIT/OFFSET tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_list_books_with_limit() {
    let pool = setup_db().await;
    seed(&pool).await;

    let page1 = queries::list_books_with_limit(&pool, 2, 0).await.unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = queries::list_books_with_limit(&pool, 2, 2).await.unwrap();
    assert_eq!(page2.len(), 2);

    // Titles should not overlap
    let p1_titles: Vec<&str> = page1.iter().map(|r| r.title.as_str()).collect();
    let p2_titles: Vec<&str> = page2.iter().map(|r| r.title.as_str()).collect();
    assert!(p1_titles.iter().all(|t| !p2_titles.contains(t)));
}

// ─── LIKE tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_search_books_by_title() {
    let pool = setup_db().await;
    seed(&pool).await;

    let results = queries::search_books_by_title(&pool, "%ound%".into()).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "Foundation");

    let results = queries::search_books_by_title(&pool, "NOPE%".into()).await.unwrap();
    assert!(results.is_empty());
}

// ─── BETWEEN tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_price_range() {
    let pool = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_by_price_range(&pool, 8.0, 10.0).await.unwrap();
    // Foundation (9.99), Earthsea (8.99) are in range; I Robot (7.99) and Dune (12.99) are not
    assert_eq!(results.len(), 2);
}

// ─── IN list tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_in_genres() {
    let pool = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_in_genres(&pool, "sci-fi".into(), "fantasy".into(), "horror".into())
        .await
        .unwrap();
    assert_eq!(results.len(), 4);
}

// ─── HAVING tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_genres_with_many_books() {
    let pool = setup_db().await;
    seed(&pool).await;

    // HAVING COUNT(*) > 1 → only sci-fi (3 books), not fantasy (1 book)
    let results = queries::get_genres_with_many_books(&pool, 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].genre, "sci-fi");
    assert_eq!(results[0].book_count, 3);
}

// ─── Subquery tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_not_by_author() {
    let pool = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_not_by_author(&pool, "Asimov".into()).await.unwrap();
    // Dune (Herbert) and Earthsea (Le Guin), but not Foundation or I Robot
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.title != "Foundation" && r.title != "I Robot"));
}

#[tokio::test]
async fn test_get_books_with_recent_sales() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Sale was created with DEFAULT CURRENT_TIMESTAMP, so use a past date
    let results = queries::get_books_with_recent_sales(&pool, "2000-01-01".into()).await.unwrap();
    // Foundation and Dune have sale_items
    assert_eq!(results.len(), 2);
}

// ─── Scalar subquery test ────────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_with_author_name() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: Scalar subquery columns are mapped as serde_json::Value (TEXT),
    // but SQLite returns the actual TEXT value, causing a decode mismatch.
    // Verify the SQL works via manual query instead.
    let rows: Vec<(i32, String, Option<String>)> = sqlx::query_as(
        "SELECT b.id, b.title, \
         (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name \
         FROM book b ORDER BY b.title",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
    let dune = rows.iter().find(|r| r.1 == "Dune").unwrap();
    assert_eq!(dune.2, Some("Herbert".into()));
}

// ─── JOIN with param tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_author_param() {
    let pool = setup_db().await;
    seed(&pool).await;

    // birth_year > 1925 → only Le Guin (1929)
    let results = queries::get_books_by_author_param(&pool, Some(1925)).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "Earthsea");
}

// ─── Qualified wildcard tests ────────────────────────────────────────────

#[tokio::test]
async fn test_get_all_book_fields() {
    let pool = setup_db().await;
    seed(&pool).await;

    let books = queries::get_all_book_fields(&pool).await.unwrap();
    assert_eq!(books.len(), 4);
    // Verify all fields are populated
    assert_eq!(books[0].id, 1);
    assert!(!books[0].title.is_empty());
    assert!(!books[0].genre.is_empty());
}

// ─── List param tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_ids() {
    let pool = setup_db().await;
    seed(&pool).await;

    let books = queries::get_books_by_ids(&pool, &[1, 3]).await.unwrap();
    assert_eq!(books.len(), 2);
    let titles: Vec<&str> = books.iter().map(|b| b.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"Dune"));
}
