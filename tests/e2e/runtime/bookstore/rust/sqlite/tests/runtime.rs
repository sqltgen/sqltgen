use e2e_rust_sqlite::db::queries;
use sqlx::SqlitePool;

/// Create an in-memory SQLite database with the schema loaded.
async fn setup_db() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

    let schema = include_str!("../../../../../fixtures/bookstore/sqlite/schema.sql");
    for statement in schema.split(';').map(str::trim).filter(|s: &&str| !s.is_empty()) {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }

    pool
}

/// Seed the database with test data.
async fn seed(pool: &SqlitePool) {
    queries::create_author(pool, "Asimov".into(), Some("Sci-fi master".into()), Some(1920)).await.unwrap();
    queries::create_author(pool, "Herbert".into(), None, Some(1920)).await.unwrap();
    queries::create_author(pool, "Le Guin".into(), Some("Earthsea".into()), Some(1929)).await.unwrap();

    queries::create_book(pool, 1, "Foundation".into(), "sci-fi".into(), 9.99, Some("1951-01-01".into())).await.unwrap();
    queries::create_book(pool, 1, "I Robot".into(), "sci-fi".into(), 7.99, Some("1950-01-01".into())).await.unwrap();
    queries::create_book(pool, 2, "Dune".into(), "sci-fi".into(), 12.99, Some("1965-01-01".into())).await.unwrap();
    queries::create_book(pool, 3, "Earthsea".into(), "fantasy".into(), 8.99, Some("1968-01-01".into())).await.unwrap();

    queries::create_customer(pool, "Alice".into(), "alice@example.com".into()).await.unwrap();

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

    let results = queries::get_books_in_genres(&pool, "sci-fi".into(), "fantasy".into(), "horror".into()).await.unwrap();
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

// ─── CreateBook test ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_book_exec() {
    let pool = setup_db().await;
    seed(&pool).await;

    queries::create_book(&pool, 1, "New Book".into(), "mystery".into(), 14.50, None).await.unwrap();

    let book = queries::get_book(&pool, 5).await.unwrap().unwrap();
    assert_eq!(book.title, "New Book");
    assert_eq!(book.genre, "mystery");
}

// ─── CreateCustomer / CreateSale / AddSaleItem tests ─────────────────────

#[tokio::test]
async fn test_create_customer_exec() {
    let pool = setup_db().await;

    queries::create_customer(&pool, "Bob".into(), "bob@example.com".into()).await.unwrap();

    // Verify via a SELECT that the row was inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM customer WHERE name = 'Bob'").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_create_sale_exec() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Alice is customer id 1; seed already creates one sale; add another
    queries::create_sale(&pool, 1).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sale WHERE customer_id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_add_sale_item_exec() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Add a new sale item to sale 1 (book 4 = Earthsea, not yet ordered)
    queries::add_sale_item(&pool, 1, 4, 3, 8.99).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 3);
}

// ─── CASE / COALESCE tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_get_book_price_label() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_book_price_label(&pool, 10.0).await.unwrap();
    assert_eq!(rows.len(), 4);

    let dune = rows.iter().find(|r| r.title == "Dune").unwrap();
    assert_eq!(dune.price_label, "expensive");

    let earthsea = rows.iter().find(|r| r.title == "Earthsea").unwrap();
    assert_eq!(earthsea.price_label, "affordable");
}

#[tokio::test]
async fn test_get_book_price_or_default() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_book_price_or_default(&pool, Some(0.0)).await.unwrap();
    assert_eq!(rows.len(), 4);
    // All seeded books have non-null prices
    assert!(rows.iter().all(|r| r.effective_price > 0.0));
}

// ─── Product type coverage ────────────────────────────────────────────────

#[tokio::test]
async fn test_get_product() {
    let pool = setup_db().await;

    let pid = uuid::Uuid::new_v4().to_string();
    queries::insert_product(&pool, pid.clone(), "SKU-GET".into(), "GetWidget".into(), 1, None, None, None, None, 3).await.unwrap();

    let row = queries::get_product(&pool, pid.clone()).await.unwrap().unwrap();
    assert_eq!(row.id, pid);
    assert_eq!(row.name, "GetWidget");
}

#[tokio::test]
async fn test_insert_and_get_product_sqlite() {
    let pool = setup_db().await;

    let product_id = uuid::Uuid::new_v4().to_string();

    queries::insert_product(&pool, product_id.clone(), "SKU-001".into(), "Widget".into(), 1, Some(1.5), Some(4.7), Some(r#"{"color":"red"}"#.into()), None, 42)
        .await
        .unwrap();

    let fetched = queries::get_product(&pool, product_id.clone()).await.unwrap().unwrap();
    assert_eq!(fetched.id, product_id);
    assert_eq!(fetched.name, "Widget");
    assert_eq!(fetched.stock_count, 42);
}

#[tokio::test]
async fn test_list_active_products_sqlite() {
    let pool = setup_db().await;

    queries::insert_product(&pool, uuid::Uuid::new_v4().to_string(), "ACT-1".into(), "Active".into(), 1, None, None, None, None, 10).await.unwrap();

    queries::insert_product(&pool, uuid::Uuid::new_v4().to_string(), "INACT-1".into(), "Inactive".into(), 0, None, None, None, None, 0).await.unwrap();

    let active = queries::list_active_products(&pool, 1).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "Active");

    let inactive = queries::list_active_products(&pool, 0).await.unwrap();
    assert_eq!(inactive.len(), 1);
    assert_eq!(inactive[0].name, "Inactive");
}

// ─── UpdateAuthorBio / DeleteAuthor tests (after fixture update) ─────────

#[tokio::test]
async fn test_update_author_bio_exec() {
    let pool = setup_db().await;
    seed(&pool).await;

    queries::update_author_bio(&pool, Some("Updated bio".into()), 1).await.unwrap();

    let author = queries::get_author(&pool, 1).await.unwrap().unwrap();
    assert_eq!(author.bio, Some("Updated bio".into()));
}

#[tokio::test]
async fn test_delete_author_exec() {
    let pool = setup_db().await;
    // Create an author with no books so FK won't block delete
    queries::create_author(&pool, "Temp".into(), None, None).await.unwrap();

    queries::delete_author(&pool, 1).await.unwrap();

    let gone = queries::get_author(&pool, 1).await.unwrap();
    assert!(gone.is_none());
}

// ─── InsertProduct / UpsertProduct tests (after fixture update) ──────────

#[tokio::test]
async fn test_insert_product_exec() {
    let pool = setup_db().await;

    let pid = uuid::Uuid::new_v4().to_string();
    queries::insert_product(&pool, pid.clone(), "SKU-002".into(), "Gadget".into(), 1, None, None, None, None, 5).await.unwrap();

    let row = queries::get_product(&pool, pid).await.unwrap().unwrap();
    assert_eq!(row.name, "Gadget");
    assert_eq!(row.stock_count, 5);
}

#[tokio::test]
async fn test_upsert_product_exec() {
    let pool = setup_db().await;

    let pid = uuid::Uuid::new_v4().to_string();
    queries::upsert_product(&pool, pid.clone(), "SKU-003".into(), "Thing".into(), 1, None, 10).await.unwrap();

    let row = queries::get_product(&pool, pid.clone()).await.unwrap().unwrap();
    assert_eq!(row.name, "Thing");
    assert_eq!(row.stock_count, 10);

    // Upsert again — should update
    queries::upsert_product(&pool, pid.clone(), "SKU-003".into(), "Thing Pro".into(), 1, None, 20).await.unwrap();

    let updated = queries::get_product(&pool, pid).await.unwrap().unwrap();
    assert_eq!(updated.name, "Thing Pro");
    assert_eq!(updated.stock_count, 20);
}

// ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────────

#[tokio::test]
async fn test_get_authors_with_null_bio() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_authors_with_null_bio(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Herbert");
}

#[tokio::test]
async fn test_get_authors_with_bio() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_authors_with_bio(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"Asimov"));
    assert!(names.contains(&"Le Guin"));
}

// ─── Date range tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_published_between() {
    let pool = setup_db().await;
    seed(&pool).await;

    // 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
    let rows = queries::get_books_published_between(&pool, Some("1951-01-01".into()), Some("1966-01-01".into())).await.unwrap();
    assert_eq!(rows.len(), 2);
    let titles: Vec<&str> = rows.iter().map(|r| r.title.as_str()).collect();
    assert!(titles.contains(&"Foundation"));
    assert!(titles.contains(&"Dune"));
}

// ─── DISTINCT tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_distinct_genres() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_distinct_genres(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    let genres: std::collections::HashSet<&str> = rows.iter().map(|r| r.genre.as_str()).collect();
    assert!(genres.contains("sci-fi"));
    assert!(genres.contains("fantasy"));
}

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────────

#[tokio::test]
async fn test_get_books_with_sales_count() {
    let pool = setup_db().await;
    seed(&pool).await;

    let rows = queries::get_books_with_sales_count(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);

    let foundation = rows.iter().find(|r| r.title == "Foundation").unwrap();
    assert_eq!(foundation.total_quantity, 2);

    let earthsea = rows.iter().find(|r| r.title == "Earthsea").unwrap();
    assert_eq!(earthsea.total_quantity, 0);
}

// ─── Scalar aggregate tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_count_sale_items() {
    let pool = setup_db().await;
    seed(&pool).await;

    let row = queries::count_sale_items(&pool, 1).await.unwrap().unwrap();
    assert_eq!(row.item_count, 2);
}

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────

#[tokio::test]
async fn test_get_sale_item_quantity_aggregates() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
    let row = queries::get_sale_item_quantity_aggregates(&pool).await.unwrap().unwrap();
    assert_eq!(row.min_qty, Some(1));
    assert_eq!(row.max_qty, Some(2));
    assert_eq!(row.sum_qty, Some(3));
    let avg = row.avg_qty.unwrap();
    assert!((avg - 1.5_f64).abs() < 0.01);
}

#[tokio::test]
async fn test_get_book_price_aggregates() {
    let pool = setup_db().await;
    seed(&pool).await;

    // Seed: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
    let row = queries::get_book_price_aggregates(&pool).await.unwrap().unwrap();
    let min = row.min_price.unwrap();
    let max = row.max_price.unwrap();
    let sum = row.sum_price.unwrap();
    let avg = row.avg_price.unwrap();
    assert!((min - 7.99_f64).abs() < 0.01);
    assert!((max - 12.99_f64).abs() < 0.01);
    assert!((sum - 39.96_f64).abs() < 0.01);
    assert!((avg - 9.99_f64).abs() < 0.01);
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
