use super::_sqltgen::DbPool;

use super::author::Author;
use super::book::Book;
use super::product::Product;

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
    pub price_label: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceOrDefaultRow {
    pub id: i32,
    pub title: String,
    pub effective_price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetGenresWithManyBooksRow {
    pub genre: String,
    pub book_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksByAuthorParamRow {
    pub id: i32,
    pub title: String,
    pub price: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksNotByAuthorRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksWithRecentSalesRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookWithAuthorNameRow {
    pub id: i32,
    pub title: String,
    pub author_name: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetAuthorStatsRow {
    pub id: i32,
    pub name: String,
    pub num_books: i64,
    pub total_sold: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListActiveProductsRow {
    pub id: String,
    pub sku: String,
    pub name: String,
    pub active: i32,
    pub weight_kg: Option<f32>,
    pub rating: Option<f32>,
    pub metadata: Option<String>,
    pub created_at: String,
    pub stock_count: i32,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetAuthorsWithNullBioRow {
    pub id: i32,
    pub name: String,
    pub birth_year: Option<i32>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksPublishedBetweenRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub published_at: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetDistinctGenresRow {
    pub genre: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBooksWithSalesCountRow {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub total_quantity: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountSaleItemsRow {
    pub item_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetSaleItemQuantityAggregatesRow {
    pub min_qty: Option<i32>,
    pub max_qty: Option<i32>,
    pub sum_qty: Option<i64>,
    pub avg_qty: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBookPriceAggregatesRow {
    pub min_price: Option<f64>,
    pub max_price: Option<f64>,
    pub sum_price: Option<f64>,
    pub avg_price: Option<f64>,
}

pub async fn create_author(pool: &DbPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO author (name, bio, birth_year)
        VALUES (?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_author(pool: &DbPool, id: i32) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &DbPool) -> Result<Vec<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        ORDER BY name
    "##;
    sqlx::query_as::<_, Author>(sql)
        .fetch_all(pool)
        .await
}

pub async fn create_book(pool: &DbPool, author_id: i32, title: String, genre: String, price: f64, published_at: Option<String>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES (?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_book(pool: &DbPool, id: i32) -> Result<Option<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_books_by_ids(pool: &DbPool, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id IN (SELECT value FROM json_each(?))
        ORDER BY title
    "##;
    let ids_json = format!("[{}]", ids.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","));
    sqlx::query_as::<_, Book>(sql)
        .bind(ids_json)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre(pool: &DbPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE genre = ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre_or_all(pool: &DbPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE ? = 'all' OR genre = ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre.clone())
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &DbPool, name: String, email: String) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO customer (name, email)
        VALUES (?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(email)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_sale(pool: &DbPool, customer_id: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO sale (customer_id)
        VALUES (?)
    "##;
    sqlx::query(sql)
        .bind(customer_id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn add_sale_item(pool: &DbPool, sale_id: i32, book_id: i32, quantity: i32, unit_price: f64) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
        VALUES (?, ?, ?, ?)
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

pub async fn list_books_with_author(pool: &DbPool) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
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

pub async fn get_books_never_ordered(pool: &DbPool) -> Result<Vec<Book>, sqlx::Error> {
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

pub async fn get_top_selling_books(pool: &DbPool) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
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

pub async fn get_best_customers(pool: &DbPool) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
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

pub async fn count_books_by_genre(pool: &DbPool) -> Result<Vec<CountBooksByGenreRow>, sqlx::Error> {
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

pub async fn list_books_with_limit(pool: &DbPool, limit: i64, offset: i64) -> Result<Vec<ListBooksWithLimitRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        ORDER BY title
        LIMIT ? OFFSET ?
    "##;
    sqlx::query_as::<_, ListBooksWithLimitRow>(sql)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
}

pub async fn search_books_by_title(pool: &DbPool, title: String) -> Result<Vec<SearchBooksByTitleRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE title LIKE ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, SearchBooksByTitleRow>(sql)
        .bind(title)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_price_range(pool: &DbPool, price: f64, price_2: f64) -> Result<Vec<GetBooksByPriceRangeRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE price BETWEEN ? AND ?
        ORDER BY price
    "##;
    sqlx::query_as::<_, GetBooksByPriceRangeRow>(sql)
        .bind(price)
        .bind(price_2)
        .fetch_all(pool)
        .await
}

pub async fn get_books_in_genres(pool: &DbPool, genre: String, genre_2: String, genre_3: String) -> Result<Vec<GetBooksInGenresRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price
        FROM book
        WHERE genre IN (?, ?, ?)
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksInGenresRow>(sql)
        .bind(genre)
        .bind(genre_2)
        .bind(genre_3)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_label(pool: &DbPool, price: f64) -> Result<Vec<GetBookPriceLabelRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, price,
               CASE WHEN price > ? THEN 'expensive' ELSE 'affordable' END AS price_label
        FROM book
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBookPriceLabelRow>(sql)
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn get_book_price_or_default(pool: &DbPool, price: Option<f64>) -> Result<Vec<GetBookPriceOrDefaultRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, COALESCE(price, ?) AS effective_price
        FROM book
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBookPriceOrDefaultRow>(sql)
        .bind(price)
        .fetch_all(pool)
        .await
}

pub async fn delete_book_by_id(pool: &DbPool, id: i32) -> Result<u64, sqlx::Error> {
    let sql = r##"
        DELETE FROM book WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(id)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
}

pub async fn get_genres_with_many_books(pool: &DbPool, count: i64) -> Result<Vec<GetGenresWithManyBooksRow>, sqlx::Error> {
    let sql = r##"
        SELECT genre, COUNT(*) AS book_count
        FROM book
        GROUP BY genre
        HAVING COUNT(*) > ?
        ORDER BY genre
    "##;
    sqlx::query_as::<_, GetGenresWithManyBooksRow>(sql)
        .bind(count)
        .fetch_all(pool)
        .await
}

pub async fn get_books_by_author_param(pool: &DbPool, birth_year: Option<i32>) -> Result<Vec<GetBooksByAuthorParamRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title, b.price
        FROM book b
        JOIN author a ON a.id = b.author_id AND a.birth_year > ?
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, GetBooksByAuthorParamRow>(sql)
        .bind(birth_year)
        .fetch_all(pool)
        .await
}

pub async fn get_all_book_fields(pool: &DbPool) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT b.*
        FROM book b
        ORDER BY b.id
    "##;
    sqlx::query_as::<_, Book>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_not_by_author(pool: &DbPool, name: String) -> Result<Vec<GetBooksNotByAuthorRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre
        FROM book
        WHERE author_id NOT IN (SELECT id FROM author WHERE name = ?)
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksNotByAuthorRow>(sql)
        .bind(name)
        .fetch_all(pool)
        .await
}

pub async fn get_books_with_recent_sales(pool: &DbPool, ordered_at: serde_json::Value) -> Result<Vec<GetBooksWithRecentSalesRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre
        FROM book
        WHERE EXISTS (
            SELECT 1 FROM sale_item si
            JOIN sale s ON s.id = si.sale_id
            WHERE si.book_id = book.id AND s.ordered_at > ?
        )
        ORDER BY title
    "##;
    sqlx::query_as::<_, GetBooksWithRecentSalesRow>(sql)
        .bind(ordered_at)
        .fetch_all(pool)
        .await
}

pub async fn get_book_with_author_name(pool: &DbPool) -> Result<Vec<GetBookWithAuthorNameRow>, sqlx::Error> {
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

pub async fn get_author_stats(pool: &DbPool) -> Result<Vec<GetAuthorStatsRow>, sqlx::Error> {
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

pub async fn get_product(pool: &DbPool, id: String) -> Result<Option<Product>, sqlx::Error> {
    let sql = r##"
        SELECT id, sku, name, active, weight_kg, rating, metadata,
               thumbnail, created_at, stock_count
        FROM product
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Product>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_active_products(pool: &DbPool, active: i32) -> Result<Vec<ListActiveProductsRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, sku, name, active, weight_kg, rating, metadata,
               created_at, stock_count
        FROM product
        WHERE active = ?
        ORDER BY name
    "##;
    sqlx::query_as::<_, ListActiveProductsRow>(sql)
        .bind(active)
        .fetch_all(pool)
        .await
}

pub async fn get_authors_with_null_bio(pool: &DbPool) -> Result<Vec<GetAuthorsWithNullBioRow>, sqlx::Error> {
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

pub async fn get_authors_with_bio(pool: &DbPool) -> Result<Vec<Author>, sqlx::Error> {
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

pub async fn get_books_published_between(pool: &DbPool, published_at: Option<String>, published_at_2: Option<String>) -> Result<Vec<GetBooksPublishedBetweenRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, genre, price, published_at
        FROM book
        WHERE published_at IS NOT NULL
          AND published_at BETWEEN ? AND ?
        ORDER BY published_at
    "##;
    sqlx::query_as::<_, GetBooksPublishedBetweenRow>(sql)
        .bind(published_at)
        .bind(published_at_2)
        .fetch_all(pool)
        .await
}

pub async fn get_distinct_genres(pool: &DbPool) -> Result<Vec<GetDistinctGenresRow>, sqlx::Error> {
    let sql = r##"
        SELECT DISTINCT genre
        FROM book
        ORDER BY genre
    "##;
    sqlx::query_as::<_, GetDistinctGenresRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_with_sales_count(pool: &DbPool) -> Result<Vec<GetBooksWithSalesCountRow>, sqlx::Error> {
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

pub async fn count_sale_items(pool: &DbPool, sale_id: i32) -> Result<Option<CountSaleItemsRow>, sqlx::Error> {
    let sql = r##"
        SELECT COUNT(*) AS item_count
        FROM sale_item
        WHERE sale_id = ?
    "##;
    sqlx::query_as::<_, CountSaleItemsRow>(sql)
        .bind(sale_id)
        .fetch_optional(pool)
        .await
}

pub async fn update_author_bio(pool: &DbPool, bio: Option<String>, id: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        UPDATE author SET bio = ? WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(bio)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn delete_author(pool: &DbPool, id: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        DELETE FROM author WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn insert_product(pool: &DbPool, id: String, sku: String, name: String, active: i32, weight_kg: Option<f32>, rating: Option<f32>, metadata: Option<String>, thumbnail: Option<Vec<u8>>, stock_count: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO product (id, sku, name, active, weight_kg, rating, metadata, thumbnail, stock_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(id)
        .bind(sku)
        .bind(name)
        .bind(active)
        .bind(weight_kg)
        .bind(rating)
        .bind(metadata)
        .bind(thumbnail)
        .bind(stock_count)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn upsert_product(pool: &DbPool, id: String, sku: String, name: String, active: i32, metadata: Option<String>, stock_count: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO product (id, sku, name, active, metadata, stock_count)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE
            SET name        = EXCLUDED.name,
                active      = EXCLUDED.active,
                metadata    = EXCLUDED.metadata,
                stock_count = EXCLUDED.stock_count
    "##;
    sqlx::query(sql)
        .bind(id)
        .bind(sku)
        .bind(name)
        .bind(active)
        .bind(metadata)
        .bind(stock_count)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_sale_item_quantity_aggregates(pool: &DbPool) -> Result<Option<GetSaleItemQuantityAggregatesRow>, sqlx::Error> {
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

pub async fn get_book_price_aggregates(pool: &DbPool) -> Result<Option<GetBookPriceAggregatesRow>, sqlx::Error> {
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

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn create_author(&self, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
        create_author(self.pool, name, bio, birth_year).await
    }

    pub async fn get_author(&self, id: i32) -> Result<Option<Author>, sqlx::Error> {
        get_author(self.pool, id).await
    }

    pub async fn list_authors(&self) -> Result<Vec<Author>, sqlx::Error> {
        list_authors(self.pool).await
    }

    pub async fn create_book(&self, author_id: i32, title: String, genre: String, price: f64, published_at: Option<String>) -> Result<(), sqlx::Error> {
        create_book(self.pool, author_id, title, genre, price, published_at).await
    }

    pub async fn get_book(&self, id: i32) -> Result<Option<Book>, sqlx::Error> {
        get_book(self.pool, id).await
    }

    pub async fn get_books_by_ids(&self, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
        get_books_by_ids(self.pool, ids).await
    }

    pub async fn list_books_by_genre(&self, genre: String) -> Result<Vec<Book>, sqlx::Error> {
        list_books_by_genre(self.pool, genre).await
    }

    pub async fn list_books_by_genre_or_all(&self, genre: String) -> Result<Vec<Book>, sqlx::Error> {
        list_books_by_genre_or_all(self.pool, genre).await
    }

    pub async fn create_customer(&self, name: String, email: String) -> Result<(), sqlx::Error> {
        create_customer(self.pool, name, email).await
    }

    pub async fn create_sale(&self, customer_id: i32) -> Result<(), sqlx::Error> {
        create_sale(self.pool, customer_id).await
    }

    pub async fn add_sale_item(&self, sale_id: i32, book_id: i32, quantity: i32, unit_price: f64) -> Result<(), sqlx::Error> {
        add_sale_item(self.pool, sale_id, book_id, quantity, unit_price).await
    }

    pub async fn list_books_with_author(&self) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
        list_books_with_author(self.pool).await
    }

    pub async fn get_books_never_ordered(&self) -> Result<Vec<Book>, sqlx::Error> {
        get_books_never_ordered(self.pool).await
    }

    pub async fn get_top_selling_books(&self) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
        get_top_selling_books(self.pool).await
    }

    pub async fn get_best_customers(&self) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
        get_best_customers(self.pool).await
    }

    pub async fn count_books_by_genre(&self) -> Result<Vec<CountBooksByGenreRow>, sqlx::Error> {
        count_books_by_genre(self.pool).await
    }

    pub async fn list_books_with_limit(&self, limit: i64, offset: i64) -> Result<Vec<ListBooksWithLimitRow>, sqlx::Error> {
        list_books_with_limit(self.pool, limit, offset).await
    }

    pub async fn search_books_by_title(&self, title: String) -> Result<Vec<SearchBooksByTitleRow>, sqlx::Error> {
        search_books_by_title(self.pool, title).await
    }

    pub async fn get_books_by_price_range(&self, price: f64, price_2: f64) -> Result<Vec<GetBooksByPriceRangeRow>, sqlx::Error> {
        get_books_by_price_range(self.pool, price, price_2).await
    }

    pub async fn get_books_in_genres(&self, genre: String, genre_2: String, genre_3: String) -> Result<Vec<GetBooksInGenresRow>, sqlx::Error> {
        get_books_in_genres(self.pool, genre, genre_2, genre_3).await
    }

    pub async fn get_book_price_label(&self, price: f64) -> Result<Vec<GetBookPriceLabelRow>, sqlx::Error> {
        get_book_price_label(self.pool, price).await
    }

    pub async fn get_book_price_or_default(&self, price: Option<f64>) -> Result<Vec<GetBookPriceOrDefaultRow>, sqlx::Error> {
        get_book_price_or_default(self.pool, price).await
    }

    pub async fn delete_book_by_id(&self, id: i32) -> Result<u64, sqlx::Error> {
        delete_book_by_id(self.pool, id).await
    }

    pub async fn get_genres_with_many_books(&self, count: i64) -> Result<Vec<GetGenresWithManyBooksRow>, sqlx::Error> {
        get_genres_with_many_books(self.pool, count).await
    }

    pub async fn get_books_by_author_param(&self, birth_year: Option<i32>) -> Result<Vec<GetBooksByAuthorParamRow>, sqlx::Error> {
        get_books_by_author_param(self.pool, birth_year).await
    }

    pub async fn get_all_book_fields(&self) -> Result<Vec<Book>, sqlx::Error> {
        get_all_book_fields(self.pool).await
    }

    pub async fn get_books_not_by_author(&self, name: String) -> Result<Vec<GetBooksNotByAuthorRow>, sqlx::Error> {
        get_books_not_by_author(self.pool, name).await
    }

    pub async fn get_books_with_recent_sales(&self, ordered_at: serde_json::Value) -> Result<Vec<GetBooksWithRecentSalesRow>, sqlx::Error> {
        get_books_with_recent_sales(self.pool, ordered_at).await
    }

    pub async fn get_book_with_author_name(&self) -> Result<Vec<GetBookWithAuthorNameRow>, sqlx::Error> {
        get_book_with_author_name(self.pool).await
    }

    pub async fn get_author_stats(&self) -> Result<Vec<GetAuthorStatsRow>, sqlx::Error> {
        get_author_stats(self.pool).await
    }

    pub async fn get_product(&self, id: String) -> Result<Option<Product>, sqlx::Error> {
        get_product(self.pool, id).await
    }

    pub async fn list_active_products(&self, active: i32) -> Result<Vec<ListActiveProductsRow>, sqlx::Error> {
        list_active_products(self.pool, active).await
    }

    pub async fn get_authors_with_null_bio(&self) -> Result<Vec<GetAuthorsWithNullBioRow>, sqlx::Error> {
        get_authors_with_null_bio(self.pool).await
    }

    pub async fn get_authors_with_bio(&self) -> Result<Vec<Author>, sqlx::Error> {
        get_authors_with_bio(self.pool).await
    }

    pub async fn get_books_published_between(&self, published_at: Option<String>, published_at_2: Option<String>) -> Result<Vec<GetBooksPublishedBetweenRow>, sqlx::Error> {
        get_books_published_between(self.pool, published_at, published_at_2).await
    }

    pub async fn get_distinct_genres(&self) -> Result<Vec<GetDistinctGenresRow>, sqlx::Error> {
        get_distinct_genres(self.pool).await
    }

    pub async fn get_books_with_sales_count(&self) -> Result<Vec<GetBooksWithSalesCountRow>, sqlx::Error> {
        get_books_with_sales_count(self.pool).await
    }

    pub async fn count_sale_items(&self, sale_id: i32) -> Result<Option<CountSaleItemsRow>, sqlx::Error> {
        count_sale_items(self.pool, sale_id).await
    }

    pub async fn update_author_bio(&self, bio: Option<String>, id: i32) -> Result<(), sqlx::Error> {
        update_author_bio(self.pool, bio, id).await
    }

    pub async fn delete_author(&self, id: i32) -> Result<(), sqlx::Error> {
        delete_author(self.pool, id).await
    }

    pub async fn insert_product(&self, id: String, sku: String, name: String, active: i32, weight_kg: Option<f32>, rating: Option<f32>, metadata: Option<String>, thumbnail: Option<Vec<u8>>, stock_count: i32) -> Result<(), sqlx::Error> {
        insert_product(self.pool, id, sku, name, active, weight_kg, rating, metadata, thumbnail, stock_count).await
    }

    pub async fn upsert_product(&self, id: String, sku: String, name: String, active: i32, metadata: Option<String>, stock_count: i32) -> Result<(), sqlx::Error> {
        upsert_product(self.pool, id, sku, name, active, metadata, stock_count).await
    }

    pub async fn get_sale_item_quantity_aggregates(&self) -> Result<Option<GetSaleItemQuantityAggregatesRow>, sqlx::Error> {
        get_sale_item_quantity_aggregates(self.pool).await
    }

    pub async fn get_book_price_aggregates(&self) -> Result<Option<GetBookPriceAggregatesRow>, sqlx::Error> {
        get_book_price_aggregates(self.pool).await
    }
}
