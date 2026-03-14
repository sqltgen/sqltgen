use e2e_rust_mysql::db::queries;
use rust_decimal::Decimal;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use std::str::FromStr;
use time::macros::date;

// ─── Setup helpers ────────────────────────────────────────────────────────────

/// Root URL for administrative operations (CREATE / DROP DATABASE).
fn root_url() -> String {
    std::env::var("MYSQL_ROOT_URL")
        .unwrap_or_else(|_| "mysql://root:sqltgen@localhost:13306".into())
}

/// Test user URL for query execution.
fn test_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://sqltgen:sqltgen@localhost:13306".into())
}

/// Connect to the test MySQL instance, create an isolated database, and return
/// a pool whose connections are scoped to that database.
/// Each test gets its own database so tests can run independently.
async fn setup_db() -> (MySqlPool, String) {
    let db_name = format!("test_{}", uuid::Uuid::new_v4().simple());

    // Create the database using the root account
    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE `{db_name}`")).execute(&admin).await.unwrap();
    sqlx::query(&format!("GRANT ALL ON `{db_name}`.* TO 'sqltgen'@'%'")).execute(&admin).await.unwrap();
    admin.close().await;

    // Connect to the new database as the test user
    let pool = MySqlPoolOptions::new()
        .connect(&format!("{}/{db_name}", test_url()))
        .await
        .unwrap();

    // Create tables from the fixture schema
    let schema = include_str!("../../../../../fixtures/bookstore/mysql/schema.sql");
    for statement in schema.split(';').map(str::trim).filter(|s: &&str| !s.is_empty()) {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }

    (pool, db_name)
}

/// Drop the isolated database and close the pool.
async fn teardown(pool: MySqlPool, db_name: &str) {
    pool.close().await;

    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS `{db_name}`")).execute(&admin).await.unwrap();
    admin.close().await;
}

/// Insert the standard set of test fixtures.
async fn seed(pool: &MySqlPool) {
    queries::create_author(pool, "Asimov".into(), Some("Sci-fi master".into()), Some(1920)).await.unwrap();
    queries::create_author(pool, "Herbert".into(), None, Some(1920)).await.unwrap();
    queries::create_author(pool, "Le Guin".into(), Some("Earthsea".into()), Some(1929)).await.unwrap();

    let price_999 = Decimal::from_str("9.99").unwrap();
    let price_799 = Decimal::from_str("7.99").unwrap();
    let price_1299 = Decimal::from_str("12.99").unwrap();
    let price_899 = Decimal::from_str("8.99").unwrap();

    queries::create_book(pool, 1, "Foundation".into(), "sci-fi".into(), price_999, Some(date!(1951-01-01))).await.unwrap();
    queries::create_book(pool, 1, "I Robot".into(), "sci-fi".into(), price_799, Some(date!(1950-01-01))).await.unwrap();
    queries::create_book(pool, 2, "Dune".into(), "sci-fi".into(), price_1299, Some(date!(1965-01-01))).await.unwrap();
    queries::create_book(pool, 3, "Earthsea".into(), "fantasy".into(), price_899, Some(date!(1968-01-01))).await.unwrap();

    queries::create_customer(pool, "Alice".into(), "alice@example.com".into()).await.unwrap();
    queries::create_sale(pool, 1).await.unwrap();
    queries::add_sale_item(pool, 1, 1, 2, price_999).await.unwrap();
    queries::add_sale_item(pool, 1, 3, 1, price_1299).await.unwrap();
}

// ─── :exec tests (write operations without RETURNING) ────────────────────────

#[tokio::test]
async fn test_create_author_exec() {
    let (pool, db) = setup_db().await;

    queries::create_author(&pool, "Test".into(), None, None).await.unwrap();

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.name, "Test");
    assert!(author.bio.is_none());

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_update_author_bio_exec() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    queries::update_author_bio(&pool, Some("Updated bio".into()), 1).await.unwrap();

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.bio, Some("Updated bio".into()));

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_delete_author_exec() {
    let (pool, db) = setup_db().await;
    queries::create_author(&pool, "Temp".into(), None, None).await.unwrap();

    queries::delete_author(&pool, 1).await.unwrap();

    let gone = queries::get_author(&pool, 1).await.unwrap();
    assert!(gone.is_none());

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_create_book_exec() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    queries::create_book(
        &pool, 1, "New Book".into(), "mystery".into(),
        Decimal::from_str("14.50").unwrap(), None,
    ).await.unwrap();

    let book = queries::get_book(&pool, 5).await.unwrap().unwrap();
    assert_eq!(book.title, "New Book");
    assert_eq!(book.genre, "mystery");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_create_customer_exec() {
    let (pool, db) = setup_db().await;

    queries::create_customer(&pool, "Bob".into(), "bob@example.com".into()).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM customer WHERE name = 'Bob'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_create_sale_exec() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    queries::create_sale(&pool, 1).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sale WHERE customer_id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_add_sale_item_exec() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    // Add sale_item for Earthsea (book 4) to sale 1
    queries::add_sale_item(
        &pool, 1, 4, 1, Decimal::from_str("8.99").unwrap(),
    ).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 3);

    teardown(pool, &db).await;
}

// ─── CASE / COALESCE tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_price_label() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_book_price_label(&pool, Decimal::from_str("10.00").unwrap())
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);

    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.price_label, "expensive");

    let earthsea = rows.iter().find(|r| r.title == "Earthsea").unwrap();
    assert_eq!(earthsea.price_label, "affordable");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_book_price_or_default() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_book_price_or_default(
        &pool, Some(Decimal::from_str("0.00").unwrap()),
    ).await.unwrap();
    assert_eq!(rows.len(), 4);
    // All seeded books have non-null prices
    assert!(rows.iter().all(|r| r.effective_price > Decimal::ZERO));

    teardown(pool, &db).await;
}

// ─── Product type coverage ────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_product_mysql() {
    let (pool, db) = setup_db().await;

    let pid = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(&pid)
    .bind("SKU-001")
    .bind("Widget")
    .bind(true)
    .bind(10i16)
    .execute(&pool)
    .await
    .unwrap();

    let row = queries::get_product(&pool, pid.clone()).await.unwrap().unwrap();
    assert_eq!(row.id, pid);
    assert_eq!(row.name, "Widget");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_list_active_products_mysql() {
    let (pool, db) = setup_db().await;

    sqlx::query(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind("ACT-1")
    .bind("Active")
    .bind(true)
    .bind(10i16)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind("INACT-1")
    .bind("Inactive")
    .bind(false)
    .bind(0i16)
    .execute(&pool)
    .await
    .unwrap();

    let active = queries::list_active_products(&pool, true).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "Active");

    let inactive = queries::list_active_products(&pool, false).await.unwrap();
    assert_eq!(inactive.len(), 1);
    assert_eq!(inactive[0].name, "Inactive");

    teardown(pool, &db).await;
}

// ─── :one tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_author() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.name, "Asimov");
    assert_eq!(author.bio, Some("Sci-fi master".into()));
    assert_eq!(author.birth_year, Some(1920));

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_author_not_found() {
    let (pool, db) = setup_db().await;
    assert!(queries::get_author(&pool, 999).await.unwrap().is_none());
    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_book() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let book = queries::get_book(&pool, 1).await.unwrap().unwrap();
    assert_eq!(book.title, "Foundation");
    assert_eq!(book.genre, "sci-fi");
    assert_eq!(book.author_id, 1);

    teardown(pool, &db).await;
}

// ─── :many tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_authors() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let authors = queries::list_authors(&pool).await.unwrap();
    assert_eq!(authors.len(), 3);
    assert_eq!(authors[0].name, "Asimov");
    assert_eq!(authors[1].name, "Herbert");
    assert_eq!(authors[2].name, "Le Guin");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_list_books_by_genre() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let sci_fi = queries::list_books_by_genre(&pool, "sci-fi".into()).await.unwrap();
    assert_eq!(sci_fi.len(), 3);

    let fantasy = queries::list_books_by_genre(&pool, "fantasy".into()).await.unwrap();
    assert_eq!(fantasy.len(), 1);
    assert_eq!(fantasy[0].title, "Earthsea");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_list_books_by_genre_or_all() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let all = queries::list_books_by_genre_or_all(&pool, "all".into()).await.unwrap();
    assert_eq!(all.len(), 4);

    let sci_fi = queries::list_books_by_genre_or_all(&pool, "sci-fi".into()).await.unwrap();
    assert_eq!(sci_fi.len(), 3);

    teardown(pool, &db).await;
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_delete_book_by_id() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    // Book 2 (I Robot) has no sale_items
    let affected = queries::delete_book_by_id(&pool, 2).await.unwrap();
    assert_eq!(affected, 1);

    let affected = queries::delete_book_by_id(&pool, 999).await.unwrap();
    assert_eq!(affected, 0);

    teardown(pool, &db).await;
}

// ─── JOIN tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_books_with_author() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::list_books_with_author(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);

    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.author_name, "Herbert");
    assert!(dune.author_bio.is_none());

    let foundation = rows.iter().find(|r| r.title == "Foundation").unwrap();
    assert_eq!(foundation.author_name, "Asimov");
    assert_eq!(foundation.author_bio, Some("Sci-fi master".into()));

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_books_never_ordered() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let books = queries::get_books_never_ordered(&pool).await.unwrap();
    // I Robot (2) and Earthsea (4) have no sale_items
    assert_eq!(books.len(), 2);
    let titles: Vec<&str> = books.iter().map(|b| b.title.as_str()).collect();
    assert!(titles.contains(&"I Robot"));
    assert!(titles.contains(&"Earthsea"));

    teardown(pool, &db).await;
}

// ─── CTE tests ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_top_selling_books() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_top_selling_books(&pool).await.unwrap();
    assert!(!rows.is_empty());
    assert_eq!(rows[0].title, "Foundation");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_best_customers() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_best_customers(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Alice");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_author_stats() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_author_stats(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    let asimov = rows.iter().find(|r| r.name == "Asimov").unwrap();
    assert_eq!(asimov.num_books, 2);

    teardown(pool, &db).await;
}

// ─── Aggregate tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_books_by_genre() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::count_books_by_genre(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);

    let fantasy = rows.iter().find(|r| r.genre == "fantasy").unwrap();
    assert_eq!(fantasy.book_count, 1);

    let sci_fi = rows.iter().find(|r| r.genre == "sci-fi").unwrap();
    assert_eq!(sci_fi.book_count, 3);

    teardown(pool, &db).await;
}

// ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_books_with_limit() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let page1 = queries::list_books_with_limit(&pool, 2, 0).await.unwrap();
    assert_eq!(page1.len(), 2);
    let page2 = queries::list_books_with_limit(&pool, 2, 2).await.unwrap();
    assert_eq!(page2.len(), 2);

    let titles1: std::collections::HashSet<&str> = page1.iter().map(|r| r.title.as_str()).collect();
    let titles2: std::collections::HashSet<&str> = page2.iter().map(|r| r.title.as_str()).collect();
    assert!(titles1.is_disjoint(&titles2));

    teardown(pool, &db).await;
}

// ─── LIKE tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_search_books_by_title() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::search_books_by_title(&pool, "%ound%".into()).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "Foundation");

    let results = queries::search_books_by_title(&pool, "NOPE%".into()).await.unwrap();
    assert!(results.is_empty());

    teardown(pool, &db).await;
}

// ─── BETWEEN tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_price_range() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_by_price_range(
        &pool,
        Decimal::from_str("8.00").unwrap(),
        Decimal::from_str("10.00").unwrap(),
    ).await.unwrap();
    // Foundation (9.99) and Earthsea (8.99)
    assert_eq!(results.len(), 2);

    teardown(pool, &db).await;
}

// ─── IN list tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_in_genres() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_in_genres(&pool, "sci-fi".into(), "fantasy".into(), "horror".into())
        .await.unwrap();
    assert_eq!(results.len(), 4);

    teardown(pool, &db).await;
}

// ─── HAVING tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_genres_with_many_books() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::get_genres_with_many_books(&pool, 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].genre, "sci-fi");
    assert_eq!(results[0].book_count, 3);

    teardown(pool, &db).await;
}

// ─── Subquery tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_not_by_author() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_not_by_author(&pool, "Asimov".into()).await.unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.title != "Foundation" && r.title != "I Robot"));

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_books_with_recent_sales() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let results = queries::get_books_with_recent_sales(&pool, time::macros::datetime!(2000-01-01 0:00)).await.unwrap();
    // Foundation and Dune have sale_items
    assert_eq!(results.len(), 2);

    teardown(pool, &db).await;
}

// ─── Scalar subquery test ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_with_author_name() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_book_with_author_name(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);
    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.author_name, Some("Herbert".into()));

    teardown(pool, &db).await;
}

// ─── JOIN with param tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_author_param() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    // birth_year > 1925 → only Le Guin (1929) → Earthsea
    let results = queries::get_books_by_author_param(&pool, Some(1925)).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "Earthsea");

    teardown(pool, &db).await;
}

// ─── Qualified wildcard tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_get_all_book_fields() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let books = queries::get_all_book_fields(&pool).await.unwrap();
    assert_eq!(books.len(), 4);
    assert_eq!(books[0].id, 1);

    teardown(pool, &db).await;
}

// ─── List param tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_by_ids() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let books = queries::get_books_by_ids(&pool, &[1, 3]).await.unwrap();
    assert_eq!(books.len(), 2);
    let titles: Vec<&str> = books.iter().map(|b| b.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"Dune"));

    teardown(pool, &db).await;
}

// ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_get_authors_with_null_bio() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_authors_with_null_bio(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Herbert");

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_authors_with_bio() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_authors_with_bio(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"Asimov"));
    assert!(names.contains(&"Le Guin"));

    teardown(pool, &db).await;
}

// ─── Date range tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_published_between() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_books_published_between(
        &pool,
        Some(date!(1951-01-01)),
        Some(date!(1966-01-01)),
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let titles: Vec<&str> = rows.iter().map(|r| r.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"Dune"));

    teardown(pool, &db).await;
}

// ─── DISTINCT tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_distinct_genres() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_distinct_genres(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    let genres: std::collections::HashSet<&str> = rows.iter().map(|r| r.genre.as_str()).collect();
    assert!(genres.contains("sci-fi"));
    assert!(genres.contains("fantasy"));

    teardown(pool, &db).await;
}

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_with_sales_count() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_books_with_sales_count(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);

    let foundation = rows.iter().find(|r| r.title == "Foundation").unwrap();
    assert_eq!(foundation.total_quantity, Decimal::from(2));

    let earthsea = rows.iter().find(|r| r.title == "Earthsea").unwrap();
    assert_eq!(earthsea.total_quantity, Decimal::ZERO);

    teardown(pool, &db).await;
}

// ─── Scalar aggregate tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_count_sale_items() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    let row = queries::count_sale_items(&pool, 1).await.unwrap().unwrap();
    assert_eq!(row.item_count, 2);

    teardown(pool, &db).await;
}

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

#[tokio::test]
async fn test_get_sale_item_quantity_aggregates() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
    let row = queries::get_sale_item_quantity_aggregates(&pool).await.unwrap().unwrap();
    assert_eq!(row.min_qty, Some(1));
    assert_eq!(row.max_qty, Some(2));
    assert_eq!(row.sum_qty, Some(Decimal::from(3)));
    let avg = row.avg_qty.unwrap();
    assert!((avg - Decimal::from_str("1.5").unwrap()).abs() < Decimal::from_str("0.01").unwrap());

    teardown(pool, &db).await;
}

#[tokio::test]
async fn test_get_book_price_aggregates() {
    let (pool, db) = setup_db().await;
    seed(&pool).await;

    // Seed: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
    let row = queries::get_book_price_aggregates(&pool).await.unwrap().unwrap();
    assert_eq!(row.min_price, Some(Decimal::from_str("7.99").unwrap()));
    assert_eq!(row.max_price, Some(Decimal::from_str("12.99").unwrap()));
    assert_eq!(row.sum_price, Some(Decimal::from_str("39.96").unwrap()));
    let avg = row.avg_price.unwrap();
    assert!((avg - Decimal::from_str("9.99").unwrap()).abs() < Decimal::from_str("0.01").unwrap());

    teardown(pool, &db).await;
}
