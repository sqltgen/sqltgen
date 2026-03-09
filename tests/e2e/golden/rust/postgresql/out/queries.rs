use sqlx::PgPool;

use super::author::Author;
use super::book::Book;

#[derive(Debug, sqlx::FromRow)]
pub struct DeleteAuthorRow {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CreateCustomerRow {
    pub id: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CreateSaleRow {
    pub id: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithAuthorRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub published_at: Option<time::Date>,
    pub author_name: String,
    pub author_bio: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetTopSellingBooksRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub units_sold: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBestCustomersRow {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub total_spent: Option<rust_decimal::Decimal>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountBooksByGenreRow {
    pub genre: String,
    pub book_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithLimitRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct SearchBooksByTitleRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksByPriceRangeRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksInGenresRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceLabelRow {
    pub id: i64,
    pub title: String,
    pub price: rust_decimal::Decimal,
    pub price_label: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceOrDefaultRow {
    pub id: i64,
    pub title: String,
    pub effective_price: Option<serde_json::Value>,
}

pub async fn create_author(pool: &PgPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("INSERT INTO author (name, bio, birth_year) VALUES ($1, $2, $3) RETURNING *")
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .fetch_optional(pool)
        .await
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

pub async fn update_author_bio(pool: &PgPool, bio: Option<String>, id: i64) -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>("UPDATE author SET bio = $1 WHERE id = $2 RETURNING *")
        .bind(bio)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn delete_author(pool: &PgPool, id: i64) -> Result<Option<DeleteAuthorRow>, sqlx::Error> {
    sqlx::query_as::<_, DeleteAuthorRow>("DELETE FROM author WHERE id = $1 RETURNING id, name")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn create_book(pool: &PgPool, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<Option<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("INSERT INTO book (author_id, title, genre, price, published_at) VALUES ($1, $2, $3, $4, $5) RETURNING *")
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .fetch_optional(pool)
        .await
}

pub async fn get_book(pool: &PgPool, id: i64) -> Result<Option<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_books_by_ids(pool: &PgPool, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ANY($1) ORDER BY title")
        .bind(ids)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre(pool: &PgPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = $1 ORDER BY title")
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre_or_all(pool: &PgPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>("SELECT id, author_id, title, genre, price, published_at FROM book WHERE $1 = 'all' OR genre = $1 ORDER BY title")
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &PgPool, name: String, email: String) -> Result<Option<CreateCustomerRow>, sqlx::Error> {
    sqlx::query_as::<_, CreateCustomerRow>("INSERT INTO customer (name, email) VALUES ($1, $2) RETURNING id")
        .bind(name)
        .bind(email)
        .fetch_optional(pool)
        .await
}

pub async fn create_sale(pool: &PgPool, customer_id: i64) -> Result<Option<CreateSaleRow>, sqlx::Error> {
    sqlx::query_as::<_, CreateSaleRow>("INSERT INTO sale (customer_id) VALUES ($1) RETURNING id")
        .bind(customer_id)
        .fetch_optional(pool)
        .await
}

pub async fn add_sale_item(pool: &PgPool, sale_id: i64, book_id: i64, quantity: i32, unit_price: rust_decimal::Decimal) -> Result<(), sqlx::Error> {
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

pub async fn count_books_by_genre(pool: &PgPool) -> Result<Vec<CountBooksByGenreRow>, sqlx::Error> {
    sqlx::query_as::<_, CountBooksByGenreRow>("SELECT genre, COUNT(*) AS book_count FROM book GROUP BY genre ORDER BY genre")
        .fetch_all(pool)
        .await
}

pub async fn list_books_with_limit(pool: &PgPool, limit: i64, offset: i64) -> Result<Vec<ListBooksWithLimitRow>, sqlx::Error> {
    sqlx::query_as::<_, ListBooksWithLimitRow>("SELECT id, title, genre, price FROM book ORDER BY title LIMIT $1 OFFSET $2")
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
}

pub async fn search_books_by_title(pool: &PgPool, title: String) -> Result<Vec<SearchBooksByTitleRow>, sqlx::Error> {
    sqlx::query_as::<_, SearchBooksByTitleRow>("SELECT id, title, genre, price FROM book WHERE title LIKE $1 ORDER BY title")
        .bind(title)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_price_range(pool: &PgPool, price: rust_decimal::Decimal, price: rust_decimal::Decimal) -> Result<Vec<GetBooksByPriceRangeRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBooksByPriceRangeRow>("SELECT id, title, genre, price FROM book WHERE price BETWEEN $1 AND $2 ORDER BY price")
        .bind(price.clone())
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_books_in_genres(pool: &PgPool, genre: String, genre: String, genre: String) -> Result<Vec<GetBooksInGenresRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBooksInGenresRow>("SELECT id, title, genre, price FROM book WHERE genre IN ($1, $2, $3) ORDER BY title")
        .bind(genre.clone())
        .bind(genre.clone())
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_label(pool: &PgPool, price: rust_decimal::Decimal) -> Result<Vec<GetBookPriceLabelRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBookPriceLabelRow>("SELECT id, title, price,        CASE WHEN price > $1 THEN 'expensive' ELSE 'affordable' END AS price_label FROM book ORDER BY title")
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_or_default(pool: &PgPool, param1: String) -> Result<Vec<GetBookPriceOrDefaultRow>, sqlx::Error> {
    sqlx::query_as::<_, GetBookPriceOrDefaultRow>("SELECT id, title, COALESCE(price, $1) AS effective_price FROM book ORDER BY title")
        .bind(param1)
        .fetch_all(pool)
        .await
}
