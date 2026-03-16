use sqlx::PgPool;

use super::author::Author;
use super::book::Book;
use super::product::Product;

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
    pub price_label: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceOrDefaultRow {
    pub id: i64,
    pub title: String,
    pub effective_price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetGenresWithManyBooksRow {
    pub genre: String,
    pub book_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksByAuthorParamRow {
    pub id: i64,
    pub title: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksNotByAuthorRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksWithRecentSalesRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookWithAuthorNameRow {
    pub id: i64,
    pub title: String,
    pub author_name: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetAuthorStatsRow {
    pub id: i64,
    pub name: String,
    pub num_books: i64,
    pub total_sold: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ArchiveAndReturnBooksRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListActiveProductsRow {
    pub id: uuid::Uuid,
    pub sku: String,
    pub name: String,
    pub active: bool,
    pub weight_kg: Option<f32>,
    pub rating: Option<f64>,
    pub tags: Vec<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: time::PrimitiveDateTime,
    pub stock_count: i16,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetAuthorsWithNullBioRow {
    pub id: i64,
    pub name: String,
    pub birth_year: Option<i32>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksPublishedBetweenRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub published_at: Option<time::Date>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetDistinctGenresRow {
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksWithSalesCountRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub total_quantity: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountSaleItemsRow {
    pub item_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct UpsertProductRow {
    pub id: uuid::Uuid,
    pub sku: String,
    pub name: String,
    pub active: bool,
    pub tags: Vec<String>,
    pub stock_count: i16,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetSaleItemQuantityAggregatesRow {
    pub min_qty: Option<i32>,
    pub max_qty: Option<i32>,
    pub sum_qty: Option<i64>,
    pub avg_qty: Option<rust_decimal::Decimal>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceAggregatesRow {
    pub min_price: Option<rust_decimal::Decimal>,
    pub max_price: Option<rust_decimal::Decimal>,
    pub sum_price: Option<rust_decimal::Decimal>,
    pub avg_price: Option<rust_decimal::Decimal>,
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

pub async fn count_books_by_genre(pool: &PgPool) -> Result<Vec<CountBooksByGenreRow>, sqlx::Error> {
    let sql = r##"
        SELECT genre, COUNT(*) AS book_count
        FROM book
        GROUP BY genre
        ORDER BY genre
    "##;
    sqlx::query_as::<_, CountBooksByGenreRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn list_books_with_limit(pool: &PgPool, limit: i64, offset: i64) -> Result<Vec<ListBooksWithLimitRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        ORDER BY title
        LIMIT $1 OFFSET $2
    "##;
    sqlx::query_as::<_, ListBooksWithLimitRow>(sql)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
}

pub async fn search_books_by_title(pool: &PgPool, title: String) -> Result<Vec<SearchBooksByTitleRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE title LIKE $1
        ORDER BY title
    "##;
    sqlx::query_as::<_, SearchBooksByTitleRow>(sql)
        .bind(title)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_price_range(pool: &PgPool, price: rust_decimal::Decimal, price_2: rust_decimal::Decimal) -> Result<Vec<GetBooksByPriceRangeRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE price BETWEEN $1 AND $2
        ORDER BY price
    "##;
    sqlx::query_as::<_, GetBooksByPriceRangeRow>(sql)
        .bind(price)
        .bind(price_2)
        .fetch_all(pool)
        .await
}

pub async fn get_books_in_genres(pool: &PgPool, genre: String, genre_2: String, genre_3: String) -> Result<Vec<GetBooksInGenresRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE genre IN ($1, $2, $3)
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksInGenresRow>(sql)
        .bind(genre)
        .bind(genre_2)
        .bind(genre_3)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_label(pool: &PgPool, price: rust_decimal::Decimal) -> Result<Vec<GetBookPriceLabelRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, price,
               CASE WHEN price > $1 THEN 'expensive' ELSE 'affordable' END AS price_label
        FROM book
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBookPriceLabelRow>(sql)
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_or_default(pool: &PgPool, price: Option<rust_decimal::Decimal>) -> Result<Vec<GetBookPriceOrDefaultRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, COALESCE(price, $1) AS effective_price
        FROM book
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBookPriceOrDefaultRow>(sql)
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn delete_book_by_id(pool: &PgPool, id: i64) -> Result<u64, sqlx::Error> {
    let sql = r##"
        DELETE FROM book WHERE id = $1
    "##;
    sqlx::query(sql)
        .bind(id)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
}

pub async fn get_genres_with_many_books(pool: &PgPool, count: i64) -> Result<Vec<GetGenresWithManyBooksRow>, sqlx::Error> {
    let sql = r##"
        SELECT genre, COUNT(*) AS book_count
        FROM book
        GROUP BY genre
        HAVING COUNT(*) > $1
        ORDER BY genre
    "##;
    sqlx::query_as::<_, GetGenresWithManyBooksRow>(sql)
        .bind(count)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_author_param(pool: &PgPool, birth_year: Option<i32>) -> Result<Vec<GetBooksByAuthorParamRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title, b.price
        FROM book b
        JOIN author a ON a.id = b.author_id AND a.birth_year > $1
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, GetBooksByAuthorParamRow>(sql)
        .bind(birth_year)
        .fetch_all(pool)
        .await
}

pub async fn get_all_book_fields(pool: &PgPool) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT b.*
        FROM book b
        ORDER BY b.id
    "##;
    sqlx::query_as::<_, Book>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_not_by_author(pool: &PgPool, name: String) -> Result<Vec<GetBooksNotByAuthorRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre
        FROM book
        WHERE author_id NOT IN (SELECT id FROM author WHERE name = $1)
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksNotByAuthorRow>(sql)
        .bind(name)
        .fetch_all(pool)
        .await
}

pub async fn get_books_with_recent_sales(pool: &PgPool, ordered_at: time::PrimitiveDateTime) -> Result<Vec<GetBooksWithRecentSalesRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre
        FROM book
        WHERE EXISTS (
            SELECT 1 FROM sale_item si
            JOIN sale s ON s.id = si.sale_id
            WHERE si.book_id = book.id AND s.ordered_at > $1
        )
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksWithRecentSalesRow>(sql)
        .bind(ordered_at)
        .fetch_all(pool)
        .await
}

pub async fn get_book_with_author_name(pool: &PgPool) -> Result<Vec<GetBookWithAuthorNameRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title,
               (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name
        FROM book b
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, GetBookWithAuthorNameRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_author_stats(pool: &PgPool) -> Result<Vec<GetAuthorStatsRow>, sqlx::Error> {
    let sql = r##"
        WITH book_counts AS (
            SELECT author_id, COUNT(*) AS num_books
            FROM book
            GROUP BY author_id
        ),
        sale_counts AS (
            SELECT b.author_id, SUM(si.quantity) AS total_sold
            FROM sale_item si
            JOIN book b ON b.id = si.book_id
            GROUP BY b.author_id
        )
        SELECT a.id, a.name,
               COALESCE(bc.num_books, 0) AS num_books,
               COALESCE(sc.total_sold, 0) AS total_sold
        FROM author a
        LEFT JOIN book_counts bc ON bc.author_id = a.id
        LEFT JOIN sale_counts sc ON sc.author_id = a.id
        ORDER BY a.name
    "##;
    sqlx::query_as::<_, GetAuthorStatsRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn archive_and_return_books(pool: &PgPool, published_at: Option<time::Date>) -> Result<Vec<ArchiveAndReturnBooksRow>, sqlx::Error> {
    let sql = r##"
        WITH archived AS (
            DELETE FROM book
            WHERE published_at < $1
            RETURNING id, title, genre, price
        )
        SELECT id, title, genre, price FROM archived ORDER BY title
    "##;
    sqlx::query_as::<_, ArchiveAndReturnBooksRow>(sql)
        .bind(published_at)
        .fetch_all(pool)
        .await
}

pub async fn get_product(pool: &PgPool, id: uuid::Uuid) -> Result<Option<Product>, sqlx::Error> {
    let sql = r##"
        SELECT id, sku, name, active, weight_kg, rating, tags, metadata,
               thumbnail, created_at, stock_count
        FROM product
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Product>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_active_products(pool: &PgPool, active: bool) -> Result<Vec<ListActiveProductsRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, sku, name, active, weight_kg, rating, tags, metadata,
               created_at, stock_count
        FROM product
        WHERE active = $1
        ORDER BY name
    "##;
    sqlx::query_as::<_, ListActiveProductsRow>(sql)
        .bind(active)
        .fetch_all(pool)
        .await
}

pub async fn insert_product(pool: &PgPool, id: uuid::Uuid, sku: String, name: String, active: bool, weight_kg: Option<f32>, rating: Option<f64>, tags: Vec<String>, metadata: Option<serde_json::Value>, thumbnail: Option<Vec<u8>>, stock_count: i16) -> Result<Option<Product>, sqlx::Error> {
    let sql = r##"
        INSERT INTO product (id, sku, name, active, weight_kg, rating, tags, metadata, thumbnail, stock_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING *
    "##;
    sqlx::query_as::<_, Product>(sql)
        .bind(id)
        .bind(sku)
        .bind(name)
        .bind(active)
        .bind(weight_kg)
        .bind(rating)
        .bind(tags)
        .bind(metadata)
        .bind(thumbnail)
        .bind(stock_count)
        .fetch_optional(pool)
        .await
}

pub async fn get_authors_with_null_bio(pool: &PgPool) -> Result<Vec<GetAuthorsWithNullBioRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, birth_year
        FROM author
        WHERE bio IS NULL
        ORDER BY name
    "##;
    sqlx::query_as::<_, GetAuthorsWithNullBioRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_authors_with_bio(pool: &PgPool) -> Result<Vec<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        WHERE bio IS NOT NULL
        ORDER BY name
    "##;
    sqlx::query_as::<_, Author>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_published_between(pool: &PgPool, published_at: Option<time::Date>, published_at_2: Option<time::Date>) -> Result<Vec<GetBooksPublishedBetweenRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price, published_at
        FROM book
        WHERE published_at IS NOT NULL
          AND published_at BETWEEN $1 AND $2
        ORDER BY published_at
    "##;
    sqlx::query_as::<_, GetBooksPublishedBetweenRow>(sql)
        .bind(published_at)
        .bind(published_at_2)
        .fetch_all(pool)
        .await
}

pub async fn get_distinct_genres(pool: &PgPool) -> Result<Vec<GetDistinctGenresRow>, sqlx::Error> {
    let sql = r##"
        SELECT DISTINCT genre
        FROM book
        ORDER BY genre
    "##;
    sqlx::query_as::<_, GetDistinctGenresRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_with_sales_count(pool: &PgPool) -> Result<Vec<GetBooksWithSalesCountRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title, b.genre,
               COALESCE(SUM(si.quantity), 0) AS total_quantity
        FROM book b
        LEFT JOIN sale_item si ON si.book_id = b.id
        GROUP BY b.id, b.title, b.genre
        ORDER BY total_quantity DESC, b.title
    "##;
    sqlx::query_as::<_, GetBooksWithSalesCountRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn count_sale_items(pool: &PgPool, sale_id: i64) -> Result<Option<CountSaleItemsRow>, sqlx::Error> {
    let sql = r##"
        SELECT COUNT(*) AS item_count
        FROM sale_item
        WHERE sale_id = $1
    "##;
    sqlx::query_as::<_, CountSaleItemsRow>(sql)
        .bind(sale_id)
        .fetch_optional(pool)
        .await
}

pub async fn upsert_product(pool: &PgPool, id: uuid::Uuid, sku: String, name: String, active: bool, tags: Vec<String>, stock_count: i16) -> Result<Option<UpsertProductRow>, sqlx::Error> {
    let sql = r##"
        INSERT INTO product (id, sku, name, active, tags, stock_count)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO UPDATE
            SET name        = EXCLUDED.name,
                active      = EXCLUDED.active,
                tags        = EXCLUDED.tags,
                stock_count = EXCLUDED.stock_count
        RETURNING id, sku, name, active, tags, stock_count
    "##;
    sqlx::query_as::<_, UpsertProductRow>(sql)
        .bind(id)
        .bind(sku)
        .bind(name)
        .bind(active)
        .bind(tags)
        .bind(stock_count)
        .fetch_optional(pool)
        .await
}

pub async fn get_sale_item_quantity_aggregates(pool: &PgPool) -> Result<Option<GetSaleItemQuantityAggregatesRow>, sqlx::Error> {
    let sql = r##"
        SELECT MIN(quantity)  AS min_qty,
               MAX(quantity)  AS max_qty,
               SUM(quantity)  AS sum_qty,
               AVG(quantity)  AS avg_qty
        FROM sale_item
    "##;
    sqlx::query_as::<_, GetSaleItemQuantityAggregatesRow>(sql)
        .fetch_optional(pool)
        .await
}

pub async fn get_book_price_aggregates(pool: &PgPool) -> Result<Option<GetBookPriceAggregatesRow>, sqlx::Error> {
    let sql = r##"
        SELECT MIN(price)  AS min_price,
               MAX(price)  AS max_price,
               SUM(price)  AS sum_price,
               AVG(price)  AS avg_price
        FROM book
    "##;
    sqlx::query_as::<_, GetBookPriceAggregatesRow>(sql)
        .fetch_optional(pool)
        .await
}
