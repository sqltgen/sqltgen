mod db;

use db::queries as q;
use rust_decimal::Decimal;

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}

async fn run_demo(pool: &sqlx::MySqlPool) -> Result<(), Box<dyn std::error::Error>> {
    // Insert authors (MySQL has no RETURNING; IDs are auto-assigned 1, 2, 3)
    q::create_author(pool, "Ursula K. Le Guin".into(), Some("Science fiction and fantasy author".into()), Some(1929)).await?;
    q::create_author(pool, "Frank Herbert".into(), Some("Author of the Dune series".into()), Some(1920)).await?;
    q::create_author(pool, "Isaac Asimov".into(), None, Some(1920)).await?;
    println!("[mysql] inserted 3 authors (ids: 1, 2, 3)");

    // Insert books (IDs 1–5)
    q::create_book(pool, 1, "The Left Hand of Darkness".into(), "sci-fi".into(), d("12.99"), None).await?;
    q::create_book(pool, 1, "The Dispossessed".into(), "sci-fi".into(), d("11.50"), None).await?;
    q::create_book(pool, 2, "Dune".into(), "sci-fi".into(), d("14.99"), None).await?;
    q::create_book(pool, 3, "Foundation".into(), "sci-fi".into(), d("10.99"), None).await?;
    q::create_book(pool, 3, "The Caves of Steel".into(), "sci-fi".into(), d("9.99"), None).await?;
    println!("[mysql] inserted 5 books");

    // Insert customers (IDs 1, 2)
    q::create_customer(pool, "Alice".into(), "alice@example.com".into()).await?;
    q::create_customer(pool, "Bob".into(), "bob@example.com".into()).await?;
    println!("[mysql] inserted 2 customers");

    // Insert sales and items (sale IDs 1, 2; book IDs: dune=3, found=4, lhod=1)
    q::create_sale(pool, 1).await?;
    q::add_sale_item(pool, 1, 3, 2, d("14.99")).await?;
    q::add_sale_item(pool, 1, 4, 1, d("10.99")).await?;
    q::create_sale(pool, 2).await?;
    q::add_sale_item(pool, 2, 3, 1, d("14.99")).await?;
    q::add_sale_item(pool, 2, 1, 1, d("12.99")).await?;
    println!("[mysql] inserted 2 sales with items");

    let authors = q::list_authors(pool).await?;
    println!("[mysql] list_authors: {} row(s)", authors.len());

    let by_ids = q::get_books_by_ids(pool, &[1, 3]).await?;
    println!("[mysql] get_books_by_ids([1,3]): {} row(s)", by_ids.len());
    for b in &by_ids {
        println!("  \"{}\"", b.title);
    }

    let books = q::list_books_by_genre(pool, "sci-fi".into()).await?;
    println!("[mysql] list_books_by_genre(sci-fi): {} row(s)", books.len());

    let all_books = q::list_books_by_genre_or_all(pool, "all".into()).await?;
    println!("[mysql] list_books_by_genre_or_all(all): {} row(s) (repeated-param demo)", all_books.len());
    let scifi2 = q::list_books_by_genre_or_all(pool, "sci-fi".into()).await?;
    println!("[mysql] list_books_by_genre_or_all(sci-fi): {} row(s)", scifi2.len());

    let with_author = q::list_books_with_author(pool).await?;
    println!("[mysql] list_books_with_author:");
    for r in &with_author {
        println!("  \"{}\" by {}", r.title, r.author_name);
    }

    let never_ordered = q::get_books_never_ordered(pool).await?;
    println!("[mysql] get_books_never_ordered: {} book(s)", never_ordered.len());
    for b in &never_ordered {
        println!("  \"{}\"", b.title);
    }

    let top = q::get_top_selling_books(pool).await?;
    println!("[mysql] get_top_selling_books:");
    for r in &top {
        println!("  \"{}\" sold {:?}", r.title, r.units_sold);
    }

    let best = q::get_best_customers(pool).await?;
    println!("[mysql] get_best_customers:");
    for r in &best {
        println!("  {} spent {:?}", r.name, r.total_spent);
    }

    // Demonstrate UPDATE and DELETE with a transient author (no books → no FK violation)
    q::create_author(pool, "Temp Author".into(), None, None).await?;
    // temp author gets ID 4
    q::update_author_bio(pool, Some("Updated bio".into()), 4).await?;
    if let Some(updated) = q::get_author(pool, 4).await? {
        println!("[mysql] update_author_bio: updated \"{}\" — bio: {:?}", updated.name, updated.bio);
    }
    q::delete_author(pool, 4).await?;
    println!("[mysql] delete_author: deleted temp author (id=4)");

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
        let db_name  = format!("sqltgen_{:08x}", ts ^ std::process::id());
        // Use root to CREATE DATABASE and GRANT access to the sqltgen user.
        let root_url = "mysql://root:sqltgen_root@127.0.0.1:3307/sqltgen";
        let db_url   = format!("mysql://sqltgen:sqltgen@127.0.0.1:3307/{}", db_name);

        let root_pool = sqlx::MySqlPool::connect(root_url).await?;
        sqlx::raw_sql(&format!(
            "CREATE DATABASE `{db}`; GRANT ALL ON `{db}`.* TO 'sqltgen'@'%'",
            db = db_name
        ))
        .execute(&root_pool)
        .await?;

        let pool = sqlx::MySqlPool::connect(&db_url).await?;
        let mut entries: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "sql"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let content = std::fs::read_to_string(entry.path())?;
            for stmt in content.split(';') {
                let stmt = stmt.trim();
                if !stmt.is_empty() {
                    sqlx::raw_sql(stmt).execute(&pool).await?;
                }
            }
        }

        let result = run_demo(&pool).await;
        pool.close().await;

        let _ = sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS `{}`", db_name))
            .execute(&root_pool)
            .await;
        root_pool.close().await;

        result
    } else {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = sqlx::MySqlPool::connect(&url).await?;
        run_demo(&pool).await
    }
}
