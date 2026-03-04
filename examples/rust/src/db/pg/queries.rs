use sqlx::PgPool;

use super::author::Author;
use super::book::Book;

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithAuthorRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub published_at: Option<time::Date>,
    pub author_name: String,
    pub author_bio: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetTopSellingBooksRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub units_sold: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBestCustomersRow {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub total_spent: Option<serde_json::Value>,
}

pub async fn create_author(pool: &PgPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO author (name, bio, birth_year) VALUES ($1, $2, $3)")
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_author(pool: &PgPool, id: i64) -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("SELECT id, name, bio, birth_year FROM author WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &PgPool) -> Result<Vec<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("SELECT id, name, bio, birth_year FROM author ORDER BY name")
        .fetch_all(pool)
        .await
}

pub async fn create_book(pool: &PgPool, author_id: i64, title: String, genre: String, price: f64, published_at: Option<time::Date>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO book (author_id, title, genre, price, published_at) VALUES ($1, $2, $3, $4, $5)")
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_book(pool: &PgPool, id: i64) -> Result<Option<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_books_by_genre(pool: &PgPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = $1 ORDER BY title")
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &PgPool, name: String, email: String) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO customer (name, email) VALUES ($1, $2)")
        .bind(name)
        .bind(email)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_sale(pool: &PgPool, customer_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO sale (customer_id) VALUES ($1)")
        .bind(customer_id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn add_sale_item(pool: &PgPool, sale_id: i64, book_id: i64, quantity: i32, unit_price: f64) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES ($1, $2, $3, $4)")
        .bind(sale_id)
        .bind(book_id)
        .bind(quantity)
        .bind(unit_price)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn list_books_with_author(pool: &PgPool) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
    sqlx::query_as::<_, ListBooksWithAuthorRow>("SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title")
        .fetch_all(pool)
        .await
}

pub async fn get_books_never_ordered(pool: &PgPool) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title")
        .fetch_all(pool)
        .await
}

pub async fn get_top_selling_books(pool: &PgPool) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
    sqlx::query_as::<_, GetTopSellingBooksRow>("WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC")
        .fetch_all(pool)
        .await
}

pub async fn get_best_customers(pool: &PgPool) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBestCustomersRow>("WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC")
        .fetch_all(pool)
        .await
}
