mod db;

use db::queries as q;
use rust_decimal::Decimal;

fn d(s: &str) -> Decimal { s.parse().unwrap() }

async fn run_demo(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let le_guin  = q::create_author(pool, "Ursula K. Le Guin".into(), Some("Science fiction and fantasy author".into()), Some(1929)).await?.unwrap();
    let herbert  = q::create_author(pool, "Frank Herbert".into(), Some("Author of the Dune series".into()), Some(1920)).await?.unwrap();
    let asimov   = q::create_author(pool, "Isaac Asimov".into(), None, Some(1920)).await?.unwrap();
    println!("[pg] inserted 3 authors (ids: {}, {}, {})", le_guin.id, herbert.id, asimov.id);

    let lhod  = q::create_book(pool, le_guin.id,  "The Left Hand of Darkness".into(), "sci-fi".into(), d("12.99"), None).await?.unwrap();
    let _disp = q::create_book(pool, le_guin.id,  "The Dispossessed".into(),           "sci-fi".into(), d("11.50"), None).await?.unwrap();
    let dune  = q::create_book(pool, herbert.id,  "Dune".into(),                       "sci-fi".into(), d("14.99"), None).await?.unwrap();
    let found = q::create_book(pool, asimov.id,   "Foundation".into(),                 "sci-fi".into(), d("10.99"), None).await?.unwrap();
    let _caves = q::create_book(pool, asimov.id,  "The Caves of Steel".into(),         "sci-fi".into(), d("9.99"),  None).await?.unwrap();
    println!("[pg] inserted 5 books");

    let alice = q::create_customer(pool, "Alice".into(), "alice@example.com".into()).await?.unwrap();
    let bob   = q::create_customer(pool, "Bob".into(),   "bob@example.com".into()).await?.unwrap();
    println!("[pg] inserted 2 customers");

    let sale1 = q::create_sale(pool, alice.id).await?.unwrap();
    q::add_sale_item(pool, sale1.id, dune.id,  2, d("14.99")).await?;
    q::add_sale_item(pool, sale1.id, found.id, 1, d("10.99")).await?;
    let sale2 = q::create_sale(pool, bob.id).await?.unwrap();
    q::add_sale_item(pool, sale2.id, dune.id, 1, d("14.99")).await?;
    q::add_sale_item(pool, sale2.id, lhod.id, 1, d("12.99")).await?;
    println!("[pg] inserted 2 sales with items");

    let authors = q::list_authors(pool).await?;
    println!("[pg] list_authors: {} row(s)", authors.len());

    // Book IDs are BIGSERIAL starting at 1 on a fresh DB; 1=Left Hand, 3=Dune.
    let by_ids = q::get_books_by_ids(pool, &[1, 3]).await?;
    println!("[pg] get_books_by_ids([1,3]): {} row(s)", by_ids.len());
    for b in &by_ids {
        println!("  \"{}\"", b.title);
    }

    let books = q::list_books_by_genre(pool, "sci-fi".into()).await?;
    println!("[pg] list_books_by_genre(sci-fi): {} row(s)", books.len());

    let all_books = q::list_books_by_genre_or_all(pool, "all".into()).await?;
    println!("[pg] list_books_by_genre_or_all(all): {} row(s) (repeated-param demo)", all_books.len());
    let scifi2 = q::list_books_by_genre_or_all(pool, "sci-fi".into()).await?;
    println!("[pg] list_books_by_genre_or_all(sci-fi): {} row(s)", scifi2.len());

    let with_author = q::list_books_with_author(pool).await?;
    println!("[pg] list_books_with_author:");
    for r in &with_author {
        println!("  \"{}\" by {}", r.title, r.author_name);
    }

    let never_ordered = q::get_books_never_ordered(pool).await?;
    println!("[pg] get_books_never_ordered: {} book(s)", never_ordered.len());
    for b in &never_ordered {
        println!("  \"{}\"", b.title);
    }

    let top = q::get_top_selling_books(pool).await?;
    println!("[pg] get_top_selling_books:");
    for r in &top {
        println!("  \"{}\" sold {:?}", r.title, r.units_sold);
    }

    let best = q::get_best_customers(pool).await?;
    println!("[pg] get_best_customers:");
    for r in &best {
        println!("  {} spent {:?}", r.name, r.total_spent);
    }

    // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
    let temp = q::create_author(pool, "Temp Author".into(), None, None).await?.unwrap();
    if let Some(updated) = q::update_author_bio(pool, Some("Updated via UPDATE RETURNING".into()), temp.id).await? {
        println!("[pg] update_author_bio: updated \"{}\" — bio: {:?}", updated.name, updated.bio);
    }
    if let Some(deleted) = q::delete_author(pool, temp.id).await? {
        println!("[pg] delete_author: deleted \"{}\" (id={})", deleted.name, deleted.id);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let migrations_dir = std::env::var("MIGRATIONS_DIR").ok();

    if let Some(dir) = migrations_dir {
        let ts  = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let db_name   = format!("sqltgen_{:08x}", ts ^ std::process::id());
        let admin_url = "postgresql://sqltgen:sqltgen@localhost:5433/postgres";
        let db_url    = format!("postgresql://sqltgen:sqltgen@localhost:5433/{}", db_name);

        let admin_pool = sqlx::PgPool::connect(admin_url).await?;
        sqlx::raw_sql(&format!(r#"CREATE DATABASE "{}""#, db_name))
            .execute(&admin_pool)
            .await?;

        let pool = sqlx::PgPool::connect(&db_url).await?;
        let mut entries: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "sql"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let content = std::fs::read_to_string(entry.path())?;
            sqlx::raw_sql(&content).execute(&pool).await?;
        }

        let result = run_demo(&pool).await;
        pool.close().await;

        let _ = sqlx::raw_sql(&format!(r#"DROP DATABASE IF EXISTS "{}""#, db_name))
            .execute(&admin_pool)
            .await;
        admin_pool.close().await;

        result
    } else {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = sqlx::PgPool::connect(&url).await?;
        run_demo(&pool).await
    }
}
