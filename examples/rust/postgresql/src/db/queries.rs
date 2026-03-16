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

pub async fn create_author(pool: &PgPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        INSERT INTO author (name, bio, birth_year)
        VALUES ($1, $2, $3)
        RETURNING *
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .fetch_optional(pool)
        .await
}

pub async fn get_author(pool: &PgPool, id: i64) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &PgPool) -> Result<Vec<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        ORDER BY name
    "##;
    sqlx::query_as::<_, Author>(sql)
        .fetch_all(pool)
        .await
}

pub async fn update_author_bio(pool: &PgPool, bio: Option<String>, id: i64) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        UPDATE author SET bio = $1 WHERE id = $2
        RETURNING *
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(bio)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn delete_author(pool: &PgPool, id: i64) -> Result<Option<DeleteAuthorRow>, sqlx::Error> {
    let sql = r##"
        DELETE FROM author WHERE id = $1
        RETURNING id, name
    "##;
    sqlx::query_as::<_, DeleteAuthorRow>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn create_book(pool: &PgPool, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<Option<Book>, sqlx::Error> {
    let sql = r##"
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .fetch_optional(pool)
        .await
}

pub async fn get_book(pool: &PgPool, id: i64) -> Result<Option<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_books_by_ids(pool: &PgPool, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = ANY($1)
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(ids)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre(pool: &PgPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE genre = $1
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre_or_all(pool: &PgPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE $1 = 'all' OR genre = $1
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &PgPool, name: String, email: String) -> Result<Option<CreateCustomerRow>, sqlx::Error> {
    let sql = r##"
        INSERT INTO customer (name, email)
        VALUES ($1, $2)
        RETURNING id
    "##;
    sqlx::query_as::<_, CreateCustomerRow>(sql)
        .bind(name)
        .bind(email)
        .fetch_optional(pool)
        .await
}

pub async fn create_sale(pool: &PgPool, customer_id: i64) -> Result<Option<CreateSaleRow>, sqlx::Error> {
    let sql = r##"
        INSERT INTO sale (customer_id)
        VALUES ($1)
        RETURNING id
    "##;
    sqlx::query_as::<_, CreateSaleRow>(sql)
        .bind(customer_id)
        .fetch_optional(pool)
        .await
}

pub async fn add_sale_item(pool: &PgPool, sale_id: i64, book_id: i64, quantity: i32, unit_price: rust_decimal::Decimal) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
        VALUES ($1, $2, $3, $4)
    "##;
    sqlx::query(sql)
        .bind(sale_id)
        .bind(book_id)
        .bind(quantity)
        .bind(unit_price)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn list_books_with_author(pool: &PgPool) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title, b.genre, b.price, b.published_at,
               a.name AS author_name, a.bio AS author_bio
        FROM book b
        JOIN author a ON a.id = b.author_id
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, ListBooksWithAuthorRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_never_ordered(pool: &PgPool) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
        FROM book b
        LEFT JOIN sale_item si ON si.book_id = b.id
        WHERE si.id IS NULL
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_top_selling_books(pool: &PgPool) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
    let sql = r##"
        WITH book_sales AS (
            SELECT book_id,
                   SUM(quantity) AS units_sold
            FROM sale_item
            GROUP BY book_id
        )
        SELECT b.id, b.title, b.genre, b.price,
               bs.units_sold
        FROM book b
        JOIN book_sales bs ON bs.book_id = b.id
        ORDER BY bs.units_sold DESC
    "##;
    sqlx::query_as::<_, GetTopSellingBooksRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_best_customers(pool: &PgPool) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
    let sql = r##"
        WITH customer_spend AS (
            SELECT s.customer_id,
                   SUM(si.quantity * si.unit_price) AS total_spent
            FROM sale s
            JOIN sale_item si ON si.sale_id = s.id
            GROUP BY s.customer_id
        )
        SELECT c.id, c.name, c.email,
               cs.total_spent
        FROM customer c
        JOIN customer_spend cs ON cs.customer_id = c.id
        ORDER BY cs.total_spent DESC
    "##;
    sqlx::query_as::<_, GetBestCustomersRow>(sql)
        .fetch_all(pool)
        .await
}
