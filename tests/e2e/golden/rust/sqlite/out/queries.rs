use sqlx::SqlitePool;

use super::author::Author;
use super::book::Book;

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithAuthorRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub published_at: Option<String>,
    pub author_name: String,
    pub author_bio: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetTopSellingBooksRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub units_sold: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBestCustomersRow {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub total_spent: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountBooksByGenreRow {
    pub genre: String,
    pub book_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithLimitRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct SearchBooksByTitleRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksByPriceRangeRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksInGenresRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceLabelRow {
    pub id: i32,
    pub title: String,
    pub price: f64,
    pub price_label: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceOrDefaultRow {
    pub id: i32,
    pub title: String,
    pub effective_price: Option<serde_json::Value>,
}

pub async fn create_author(pool: &SqlitePool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO author (name, bio, birth_year) VALUES (?, ?, ?)")
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_author(pool: &SqlitePool, id: i32) -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("SELECT id, name, bio, birth_year FROM author WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &SqlitePool) -> Result<Vec<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("SELECT id, name, bio, birth_year FROM author ORDER BY name")
        .fetch_all(pool)
        .await
}

pub async fn create_book(pool: &SqlitePool, author_id: i32, title: String, genre: String, price: f64, published_at: Option<String>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO book (author_id, title, genre, price, published_at) VALUES (?, ?, ?, ?, ?)")
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_book(pool: &SqlitePool, id: i32) -> Result<Option<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_books_by_ids(pool: &SqlitePool, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
    let ids_json = format!("[{}]", ids.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","));
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE id IN (SELECT value FROM json_each(?)) ORDER BY title")
        .bind(ids_json)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre(pool: &SqlitePool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = ? ORDER BY title")
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre_or_all(pool: &SqlitePool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE ? = 'all' OR genre = ? ORDER BY title")
        .bind(genre.clone())
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &SqlitePool, name: String, email: String) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO customer (name, email) VALUES (?, ?)")
        .bind(name)
        .bind(email)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_sale(pool: &SqlitePool, customer_id: i32) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO sale (customer_id) VALUES (?)")
        .bind(customer_id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn add_sale_item(pool: &SqlitePool, sale_id: i32, book_id: i32, quantity: i32, unit_price: f64) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?, ?, ?, ?)")
        .bind(sale_id)
        .bind(book_id)
        .bind(quantity)
        .bind(unit_price)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn list_books_with_author(pool: &SqlitePool) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
    sqlx::query_as::<_, ListBooksWithAuthorRow>("SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title")
        .fetch_all(pool)
        .await
}

pub async fn get_books_never_ordered(pool: &SqlitePool) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title")
        .fetch_all(pool)
        .await
}

pub async fn get_top_selling_books(pool: &SqlitePool) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
    sqlx::query_as::<_, GetTopSellingBooksRow>("WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC")
        .fetch_all(pool)
        .await
}

pub async fn get_best_customers(pool: &SqlitePool) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBestCustomersRow>("WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC")
        .fetch_all(pool)
        .await
}

pub async fn count_books_by_genre(pool: &SqlitePool) -> Result<Vec<CountBooksByGenreRow>, sqlx::Error> {
    sqlx::query_as::<_, CountBooksByGenreRow>("SELECT genre, COUNT(*) AS book_count FROM book GROUP BY genre ORDER BY genre")
        .fetch_all(pool)
        .await
}

pub async fn list_books_with_limit(pool: &SqlitePool, limit: i64, offset: i64) -> Result<Vec<ListBooksWithLimitRow>, sqlx::Error> {
    sqlx::query_as::<_, ListBooksWithLimitRow>("SELECT id, title, genre, price FROM book ORDER BY title LIMIT ? OFFSET ?")
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
}

pub async fn search_books_by_title(pool: &SqlitePool, title: String) -> Result<Vec<SearchBooksByTitleRow>, sqlx::Error> {
    sqlx::query_as::<_, SearchBooksByTitleRow>("SELECT id, title, genre, price FROM book WHERE title LIKE ? ORDER BY title")
        .bind(title)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_price_range(pool: &SqlitePool, price: f64, price: f64) -> Result<Vec<GetBooksByPriceRangeRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBooksByPriceRangeRow>("SELECT id, title, genre, price FROM book WHERE price BETWEEN ? AND ? ORDER BY price")
        .bind(price.clone())
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_books_in_genres(pool: &SqlitePool, genre: String, genre: String, genre: String) -> Result<Vec<GetBooksInGenresRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBooksInGenresRow>("SELECT id, title, genre, price FROM book WHERE genre IN (?, ?, ?) ORDER BY title")
        .bind(genre.clone())
        .bind(genre.clone())
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_label(pool: &SqlitePool, price: f64) -> Result<Vec<GetBookPriceLabelRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBookPriceLabelRow>("SELECT id, title, price,        CASE WHEN price > ? THEN 'expensive' ELSE 'affordable' END AS price_label FROM book ORDER BY title")
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_or_default(pool: &SqlitePool, param1: String) -> Result<Vec<GetBookPriceOrDefaultRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBookPriceOrDefaultRow>("SELECT id, title, COALESCE(price, ?) AS effective_price FROM book ORDER BY title")
        .bind(param1)
        .fetch_all(pool)
        .await
}
