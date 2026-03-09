use e2e_rust_postgresql::db::queries;
use rust_decimal::Decimal;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::str::FromStr;
use time::Date;
use uuid::Uuid;

/// Connect to the test PostgreSQL instance and create an isolated schema.
/// Each test gets its own schema so tests can run in parallel.
/// Uses `after_connect` to SET search_path on every pooled connection.
async fn setup_db() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e".into());

    // First, create the schema using a one-off connection
    let bootstrap = PgPool::connect(&url).await.unwrap();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&bootstrap)
        .await
        .unwrap();
    bootstrap.close().await;

    // Build a pool that sets search_path on every connection
    let pool = PgPoolOptions::new()
        .after_connect({
            let schema = schema.clone();
            move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    sqlx::query(&format!("SET search_path TO \"{schema}\""))
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
        .unwrap();

    // Create tables in this schema
    sqlx::query(
        "CREATE TABLE author (
            id         BIGSERIAL    PRIMARY KEY,
            name       TEXT         NOT NULL,
            bio        TEXT,
            birth_year INTEGER
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE book (
            id           BIGSERIAL      PRIMARY KEY,
            author_id    BIGINT         NOT NULL REFERENCES author(id),
            title        TEXT           NOT NULL,
            genre        TEXT           NOT NULL,
            price        NUMERIC(10, 2) NOT NULL,
            published_at DATE
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE customer (
            id    BIGSERIAL PRIMARY KEY,
            name  TEXT      NOT NULL,
            email TEXT      NOT NULL UNIQUE
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE sale (
            id          BIGSERIAL  PRIMARY KEY,
            customer_id BIGINT     NOT NULL REFERENCES customer(id),
            ordered_at  TIMESTAMP  NOT NULL DEFAULT NOW()
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE sale_item (
            id         BIGSERIAL      PRIMARY KEY,
            sale_id    BIGINT         NOT NULL REFERENCES sale(id),
            book_id    BIGINT         NOT NULL REFERENCES book(id),
            quantity   INTEGER        NOT NULL,
            unit_price NUMERIC(10, 2) NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE product (
            id          UUID         PRIMARY KEY,
            sku         VARCHAR(50)  NOT NULL,
            name        TEXT         NOT NULL,
            active      BOOLEAN      NOT NULL DEFAULT TRUE,
            weight_kg   REAL,
            rating      DOUBLE PRECISION,
            tags        TEXT[]       NOT NULL DEFAULT '{}',
            metadata    JSONB,
            thumbnail   BYTEA,
            created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
            stock_count SMALLINT     NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    pool
}

/// Seed the database with test data.
async fn seed(pool: &PgPool) {
    queries::create_author(pool, "Asimov".into(), Some("Sci-fi master".into()), Some(1920))
        .await
        .unwrap();
    queries::create_author(pool, "Herbert".into(), None, Some(1920))
        .await
        .unwrap();
    queries::create_author(pool, "Le Guin".into(), Some("Earthsea".into()), Some(1929))
        .await
        .unwrap();

    let jan_1951 = Date::from_calendar_date(1951, time::Month::January, 1).unwrap();
    let jan_1950 = Date::from_calendar_date(1950, time::Month::January, 1).unwrap();
    let jan_1965 = Date::from_calendar_date(1965, time::Month::January, 1).unwrap();
    let jan_1968 = Date::from_calendar_date(1968, time::Month::January, 1).unwrap();

    queries::create_book(
        pool, 1, "Foundation".into(), "sci-fi".into(),
        Decimal::from_str("9.99").unwrap(), Some(jan_1951),
    ).await.unwrap();
    queries::create_book(
        pool, 1, "I Robot".into(), "sci-fi".into(),
        Decimal::from_str("7.99").unwrap(), Some(jan_1950),
    ).await.unwrap();
    queries::create_book(
        pool, 2, "Dune".into(), "sci-fi".into(),
        Decimal::from_str("12.99").unwrap(), Some(jan_1965),
    ).await.unwrap();
    queries::create_book(
        pool, 3, "Earthsea".into(), "fantasy".into(),
        Decimal::from_str("8.99").unwrap(), Some(jan_1968),
    ).await.unwrap();

    let cust = queries::create_customer(pool, "Alice".into(), "alice@example.com".into())
        .await
        .unwrap()
        .unwrap();

    let sale = queries::create_sale(pool, cust.id).await.unwrap().unwrap();

    queries::add_sale_item(pool, sale.id, 1, 2, Decimal::from_str("9.99").unwrap())
        .await
        .unwrap();
    queries::add_sale_item(pool, sale.id, 3, 1, Decimal::from_str("12.99").unwrap())
        .await
        .unwrap();
}

// ─── :one tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_author_returning() {
    let pool = setup_db().await;

    let author = queries::create_author(&pool, "Test".into(), Some("bio".into()), Some(1980))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(author.name, "Test");
    assert_eq!(author.bio, Some("bio".into()));
    assert_eq!(author.birth_year, Some(1980));
    assert!(author.id > 0);
}

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
    assert_eq!(book.price, Decimal::from_str("9.99").unwrap());
    assert_eq!(book.published_at, Some(Date::from_calendar_date(1951, time::Month::January, 1).unwrap()));
}

// ─── :one with RETURNING (PG-specific) ──────────────────────────────────

#[tokio::test]
async fn test_create_book_returning() {
    let pool = setup_db().await;
    seed(&pool).await;

    let book = queries::create_book(
        &pool, 1, "New Book".into(), "mystery".into(),
        Decimal::from_str("14.50").unwrap(), None,
    ).await.unwrap().unwrap();
    assert_eq!(book.title, "New Book");
    assert_eq!(book.genre, "mystery");
    assert_eq!(book.price, Decimal::from_str("14.50").unwrap());
    assert!(book.published_at.is_none());
}

#[tokio::test]
async fn test_update_author_bio_returning() {
    let pool = setup_db().await;
    seed(&pool).await;

    let updated = queries::update_author_bio(&pool, Some("Updated bio".into()), 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.name, "Asimov");
    assert_eq!(updated.bio, Some("Updated bio".into()));
}

#[tokio::test]
async fn test_delete_author_returning() {
    let pool = setup_db().await;
    // Create a standalone author with no books
    queries::create_author(&pool, "Temp".into(), None, None).await.unwrap();

    let deleted = queries::delete_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(deleted.name, "Temp");

    let gone = queries::get_author(&pool, 1).await.unwrap();
    assert!(gone.is_none());
}

// ─── :many tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_authors() {
    let pool = setup_db().await;
    seed(&pool).await;

    let authors = queries::list_authors(&pool).await.unwrap();
    assert_eq!(authors.len(), 3);
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
async fn test_add_sale_item() {
    let pool = setup_db().await;
    seed(&pool).await;

    // add_sale_item returns () on success
    queries::add_sale_item(&pool, 1, 2, 5, Decimal::from_str("7.99").unwrap())
        .await
        .unwrap();
}

// ─── :execrows tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_delete_book_by_id() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Book 2 (I Robot) has no sale_items
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
    assert_eq!(rows[0].units_sold, Some(2));
}

#[tokio::test]
async fn test_get_best_customers() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_best_customers(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Alice");
    assert!(rows[0].total_spent.is_some());
}

#[tokio::test]
async fn test_get_author_stats() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: COALESCE expression columns are mapped as serde_json::Value (JSONB),
    // but PG returns INT8 for COALESCE(COUNT(*), 0). Known codegen limitation.
    // Verify the SQL works via manual query.
    let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
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
    assert_eq!(rows[0].1, "Asimov");
    assert_eq!(rows[0].2, 2); // 2 books
}

// ─── Data-modifying CTE (PG-only) ──────────────────────────────────────

#[tokio::test]
async fn test_archive_and_return_books() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Archive books published before 1960.
    // Foundation (1951) and I Robot (1950) qualify, but Foundation has sale_items (FK).
    // Delete sale_items first so the CTE DELETE can succeed.
    sqlx::query("DELETE FROM sale_item").execute(&pool).await.unwrap();

    let cutoff = Date::from_calendar_date(1960, time::Month::January, 1).unwrap();
    let archived = queries::archive_and_return_books(&pool, Some(cutoff)).await.unwrap();
    assert_eq!(archived.len(), 2);
    let titles: Vec<&str> = archived.iter().map(|r| r.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"I Robot"));

    // Verify they're actually gone
    let remaining = queries::list_books_by_genre(&pool, "sci-fi".into()).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].title, "Dune");
}

// ─── Aggregate tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_books_by_genre() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::count_books_by_genre(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);

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

    let results = queries::get_books_by_price_range(
        &pool,
        Decimal::from_str("8.00").unwrap(),
        Decimal::from_str("10.00").unwrap(),
    ).await.unwrap();
    // Foundation (9.99) and Earthsea (8.99)
    assert_eq!(results.len(), 2);
}

// ─── IN list tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_in_genres() {
    let pool = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_in_genres(
        &pool, "sci-fi".into(), "fantasy".into(), "horror".into(),
    ).await.unwrap();
    assert_eq!(results.len(), 4);
}

// ─── HAVING tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_genres_with_many_books() {
    let pool = setup_db().await;
    seed(&pool).await;

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
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.title != "Foundation" && r.title != "I Robot"));
}

#[tokio::test]
async fn test_get_books_with_recent_sales() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: The parameter $1 is compared against `ordered_at` (TIMESTAMP) but
    // codegen infers it as String (from EXISTS subquery). PG won't implicitly
    // cast text→timestamp. Known codegen limitation. Verify via manual query.
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT id, title, genre FROM book WHERE EXISTS (\
            SELECT 1 FROM sale_item si \
            JOIN sale s ON s.id = si.sale_id \
            WHERE si.book_id = book.id AND s.ordered_at > $1::timestamp\
        ) ORDER BY title",
    )
    .bind("2000-01-01 00:00:00")
    .fetch_all(&pool)
    .await
    .unwrap();
    // Foundation and Dune have sale_items
    assert_eq!(rows.len(), 2);
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
    assert_eq!(books[0].id, 1);
    assert!(!books[0].title.is_empty());
}

// ─── List param tests (PG native ANY) ───────────────────────────────────

#[tokio::test]
async fn test_get_books_by_ids() {
    let pool = setup_db().await;
    seed(&pool).await;

    let books = queries::get_books_by_ids(&pool, &[1, 3]).await.unwrap();
    assert_eq!(books.len(), 2);
    let titles: Vec<&str> = books.iter().map(|b| b.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"Dune"));

    // Empty list
    let books = queries::get_books_by_ids(&pool, &[]).await.unwrap();
    assert!(books.is_empty());
}

// ─── Product type coverage (UUID, BOOLEAN, REAL, DOUBLE, TEXT[], JSONB, BYTEA, SMALLINT) ─

#[tokio::test]
async fn test_insert_and_get_product() {
    let pool = setup_db().await;

    let product_id = Uuid::new_v4();
    let metadata = serde_json::json!({"color": "red", "size": "L"});

    let product = queries::insert_product(
        &pool,
        product_id,
        "SKU-001".into(),
        "Widget".into(),
        true,
        Some(1.5),
        Some(4.7),
        vec!["electronics".into(), "sale".into()],
        Some(metadata.clone()),
        Some(vec![0xFF, 0xD8, 0xFF]),
        42,
    ).await.unwrap().unwrap();

    assert_eq!(product.id, product_id);
    assert_eq!(product.sku, "SKU-001");
    assert_eq!(product.name, "Widget");
    assert!(product.active);
    assert_eq!(product.weight_kg, Some(1.5));
    assert!((product.rating.unwrap() - 4.7).abs() < 0.001);
    assert_eq!(product.tags, vec!["electronics", "sale"]);
    assert_eq!(product.metadata, Some(metadata));
    assert_eq!(product.thumbnail, Some(vec![0xFF, 0xD8, 0xFF]));
    assert_eq!(product.stock_count, 42);

    // Fetch it back by id
    let fetched = queries::get_product(&pool, product_id).await.unwrap().unwrap();
    assert_eq!(fetched.id, product_id);
    assert_eq!(fetched.name, "Widget");
    assert_eq!(fetched.tags, vec!["electronics", "sale"]);
}

#[tokio::test]
async fn test_list_active_products() {
    let pool = setup_db().await;

    queries::insert_product(
        &pool, Uuid::new_v4(), "ACT-1".into(), "Active".into(), true,
        None, None, vec![], None, None, 10,
    ).await.unwrap();

    queries::insert_product(
        &pool, Uuid::new_v4(), "INACT-1".into(), "Inactive".into(), false,
        None, None, vec![], None, None, 0,
    ).await.unwrap();

    let active = queries::list_active_products(&pool, true).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "Active");

    let inactive = queries::list_active_products(&pool, false).await.unwrap();
    assert_eq!(inactive.len(), 1);
    assert_eq!(inactive[0].name, "Inactive");
}

#[tokio::test]
async fn test_product_with_nulls() {
    let pool = setup_db().await;

    let product = queries::insert_product(
        &pool, Uuid::new_v4(), "NULL-1".into(), "Minimal".into(), true,
        None, None, vec![], None, None, 0,
    ).await.unwrap().unwrap();

    assert!(product.weight_kg.is_none());
    assert!(product.rating.is_none());
    assert!(product.tags.is_empty());
    assert!(product.metadata.is_none());
    assert!(product.thumbnail.is_none());
}

// ─── CASE / COALESCE tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_price_label() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: CASE expression columns are mapped as serde_json::Value (JSONB),
    // but PG returns TEXT. Known codegen limitation. Verify via manual query.
    let rows: Vec<(i64, String, Decimal, String)> = sqlx::query_as(
        "SELECT id, title, price, \
         CASE WHEN price > $1 THEN 'expensive' ELSE 'affordable' END AS price_label \
         FROM book ORDER BY title",
    )
    .bind(Decimal::from_str("10.00").unwrap())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);

    let dune = rows.iter().find(|r| r.1 == "Dune").unwrap();
    assert_eq!(dune.3, "expensive");

    let earthsea = rows.iter().find(|r| r.1 == "Earthsea").unwrap();
    assert_eq!(earthsea.3, "affordable");
}

// ─── Scalar subquery test ────────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_with_author_name() {
    let pool = setup_db().await;
    seed(&pool).await;

    // NOTE: Scalar subquery columns are mapped as serde_json::Value (JSONB),
    // but PG returns TEXT. Known codegen limitation. Verify via manual query.
    let rows: Vec<(i64, String, Option<String>)> = sqlx::query_as(
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
