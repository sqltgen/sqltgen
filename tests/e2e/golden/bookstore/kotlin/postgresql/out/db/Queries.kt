package db

import java.sql.Connection

object Queries {

    private val SQL_CREATE_AUTHOR = """
        INSERT INTO author (name, bio, birth_year)
        VALUES (?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createAuthor(conn: Connection, name: String, bio: String?, birthYear: Int?): Author? {
        conn.prepareStatement(SQL_CREATE_AUTHOR).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, bio)
            ps.setObject(3, birthYear)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    private val SQL_GET_AUTHOR = """
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = ?;
    """.trimIndent()
    fun getAuthor(conn: Connection, id: Long): Author? {
        conn.prepareStatement(SQL_GET_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    private val SQL_LIST_AUTHORS = """
        SELECT id, name, bio, birth_year
        FROM author
        ORDER BY name;
    """.trimIndent()
    fun listAuthors(conn: Connection): List<Author> {
        conn.prepareStatement(SQL_LIST_AUTHORS).use { ps ->
            val rows = mutableListOf<Author>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)))
            }
            return rows
        }
    }

    private val SQL_UPDATE_AUTHOR_BIO = """
        UPDATE author SET bio = ? WHERE id = ?
        RETURNING *;
    """.trimIndent()
    fun updateAuthorBio(conn: Connection, bio: String?, id: Long): Author? {
        conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO).use { ps ->
            ps.setObject(1, bio)
            ps.setLong(2, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    data class DeleteAuthorRow(
        val id: Long,
        val name: String
    )

    private val SQL_DELETE_AUTHOR = """
        DELETE FROM author WHERE id = ?
        RETURNING id, name;
    """.trimIndent()
    fun deleteAuthor(conn: Connection, id: Long): DeleteAuthorRow? {
        conn.prepareStatement(SQL_DELETE_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return DeleteAuthorRow(rs.getLong(1), rs.getString(2))
            }
        }
    }

    private val SQL_CREATE_BOOK = """
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES (?, ?, ?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createBook(conn: Connection, authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Book? {
        conn.prepareStatement(SQL_CREATE_BOOK).use { ps ->
            ps.setLong(1, authorId)
            ps.setString(2, title)
            ps.setString(3, genre)
            ps.setBigDecimal(4, price)
            ps.setObject(5, publishedAt)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java))
            }
        }
    }

    private val SQL_GET_BOOK = """
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = ?;
    """.trimIndent()
    fun getBook(conn: Connection, id: Long): Book? {
        conn.prepareStatement(SQL_GET_BOOK).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java))
            }
        }
    }

    private val SQL_GET_BOOKS_BY_IDS = """
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = ANY(?)
        ORDER BY title;
    """.trimIndent()
    fun getBooksByIds(conn: Connection, ids: List<Long>): List<Book> {
        val arr = conn.createArrayOf("bigint", ids.toTypedArray())
        conn.prepareStatement(SQL_GET_BOOKS_BY_IDS).use { ps ->
            ps.setArray(1, arr)
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    private val SQL_LIST_BOOKS_BY_GENRE = """
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE genre = ?
        ORDER BY title;
    """.trimIndent()
    fun listBooksByGenre(conn: Connection, genre: String): List<Book> {
        conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE).use { ps ->
            ps.setString(1, genre)
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    private val SQL_LIST_BOOKS_BY_GENRE_OR_ALL = """
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE ? = 'all' OR genre = ?
        ORDER BY title;
    """.trimIndent()
    fun listBooksByGenreOrAll(conn: Connection, genre: String): List<Book> {
        conn.prepareStatement(SQL_LIST_BOOKS_BY_GENRE_OR_ALL).use { ps ->
            ps.setString(1, genre)
            ps.setString(2, genre)
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    data class CreateCustomerRow(
        val id: Long
    )

    private val SQL_CREATE_CUSTOMER = """
        INSERT INTO customer (name, email)
        VALUES (?, ?)
        RETURNING id;
    """.trimIndent()
    fun createCustomer(conn: Connection, name: String, email: String): CreateCustomerRow? {
        conn.prepareStatement(SQL_CREATE_CUSTOMER).use { ps ->
            ps.setString(1, name)
            ps.setString(2, email)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return CreateCustomerRow(rs.getLong(1))
            }
        }
    }

    data class CreateSaleRow(
        val id: Long
    )

    private val SQL_CREATE_SALE = """
        INSERT INTO sale (customer_id)
        VALUES (?)
        RETURNING id;
    """.trimIndent()
    fun createSale(conn: Connection, customerId: Long): CreateSaleRow? {
        conn.prepareStatement(SQL_CREATE_SALE).use { ps ->
            ps.setLong(1, customerId)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return CreateSaleRow(rs.getLong(1))
            }
        }
    }

    private val SQL_ADD_SALE_ITEM = """
        INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
        VALUES (?, ?, ?, ?);
    """.trimIndent()
    fun addSaleItem(conn: Connection, saleId: Long, bookId: Long, quantity: Int, unitPrice: java.math.BigDecimal): Unit {
        conn.prepareStatement(SQL_ADD_SALE_ITEM).use { ps ->
            ps.setLong(1, saleId)
            ps.setLong(2, bookId)
            ps.setInt(3, quantity)
            ps.setBigDecimal(4, unitPrice)
            ps.executeUpdate()
        }
    }

    data class ListBooksWithAuthorRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal,
        val publishedAt: java.time.LocalDate?,
        val authorName: String,
        val authorBio: String?
    )

    private val SQL_LIST_BOOKS_WITH_AUTHOR = """
        SELECT b.id, b.title, b.genre, b.price, b.published_at,
               a.name AS author_name, a.bio AS author_bio
        FROM book b
        JOIN author a ON a.id = b.author_id
        ORDER BY b.title;
    """.trimIndent()
    fun listBooksWithAuthor(conn: Connection): List<ListBooksWithAuthorRow> {
        conn.prepareStatement(SQL_LIST_BOOKS_WITH_AUTHOR).use { ps ->
            val rows = mutableListOf<ListBooksWithAuthorRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListBooksWithAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5, java.time.LocalDate::class.java), rs.getString(6), rs.getString(7)))
            }
            return rows
        }
    }

    private val SQL_LIST_BOOK_SUMMARIES_VIEW = """
        SELECT id, title, genre, author_name
        FROM book_summaries
        ORDER BY title;
    """.trimIndent()
    fun listBookSummariesView(conn: Connection): List<BookSummaries> {
        conn.prepareStatement(SQL_LIST_BOOK_SUMMARIES_VIEW).use { ps ->
            val rows = mutableListOf<BookSummaries>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(BookSummaries(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            }
            return rows
        }
    }

    private val SQL_GET_BOOKS_NEVER_ORDERED = """
        SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
        FROM book b
        LEFT JOIN sale_item si ON si.book_id = b.id
        WHERE si.id IS NULL
        ORDER BY b.title;
    """.trimIndent()
    fun getBooksNeverOrdered(conn: Connection): List<Book> {
        conn.prepareStatement(SQL_GET_BOOKS_NEVER_ORDERED).use { ps ->
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    data class GetTopSellingBooksRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal,
        val unitsSold: Long?
    )

    private val SQL_GET_TOP_SELLING_BOOKS = """
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
        ORDER BY bs.units_sold DESC;
    """.trimIndent()
    fun getTopSellingBooks(conn: Connection): List<GetTopSellingBooksRow> {
        conn.prepareStatement(SQL_GET_TOP_SELLING_BOOKS).use { ps ->
            val rows = mutableListOf<GetTopSellingBooksRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetTopSellingBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), getNullableLong(rs, 5)))
            }
            return rows
        }
    }

    data class GetBestCustomersRow(
        val id: Long,
        val name: String,
        val email: String,
        val totalSpent: java.math.BigDecimal?
    )

    private val SQL_GET_BEST_CUSTOMERS = """
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
        ORDER BY cs.total_spent DESC;
    """.trimIndent()
    fun getBestCustomers(conn: Connection): List<GetBestCustomersRow> {
        conn.prepareStatement(SQL_GET_BEST_CUSTOMERS).use { ps ->
            val rows = mutableListOf<GetBestCustomersRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBestCustomersRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    data class CountBooksByGenreRow(
        val genre: String,
        val bookCount: Long
    )

    private val SQL_COUNT_BOOKS_BY_GENRE = """
        SELECT genre, COUNT(*) AS book_count
        FROM book
        GROUP BY genre
        ORDER BY genre;
    """.trimIndent()
    fun countBooksByGenre(conn: Connection): List<CountBooksByGenreRow> {
        conn.prepareStatement(SQL_COUNT_BOOKS_BY_GENRE).use { ps ->
            val rows = mutableListOf<CountBooksByGenreRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(CountBooksByGenreRow(rs.getString(1), rs.getLong(2)))
            }
            return rows
        }
    }

    data class ListBooksWithLimitRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal
    )

    private val SQL_LIST_BOOKS_WITH_LIMIT = """
        SELECT id, title, genre, price
        FROM book
        ORDER BY title
        LIMIT ? OFFSET ?;
    """.trimIndent()
    fun listBooksWithLimit(conn: Connection, limit: Long, offset: Long): List<ListBooksWithLimitRow> {
        conn.prepareStatement(SQL_LIST_BOOKS_WITH_LIMIT).use { ps ->
            ps.setLong(1, limit)
            ps.setLong(2, offset)
            val rows = mutableListOf<ListBooksWithLimitRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListBooksWithLimitRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    data class SearchBooksByTitleRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal
    )

    private val SQL_SEARCH_BOOKS_BY_TITLE = """
        SELECT id, title, genre, price
        FROM book
        WHERE title LIKE ?
        ORDER BY title;
    """.trimIndent()
    fun searchBooksByTitle(conn: Connection, title: String): List<SearchBooksByTitleRow> {
        conn.prepareStatement(SQL_SEARCH_BOOKS_BY_TITLE).use { ps ->
            ps.setString(1, title)
            val rows = mutableListOf<SearchBooksByTitleRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(SearchBooksByTitleRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    data class GetBooksByPriceRangeRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal
    )

    private val SQL_GET_BOOKS_BY_PRICE_RANGE = """
        SELECT id, title, genre, price
        FROM book
        WHERE price BETWEEN ? AND ?
        ORDER BY price;
    """.trimIndent()
    fun getBooksByPriceRange(conn: Connection, price: java.math.BigDecimal, price2: java.math.BigDecimal): List<GetBooksByPriceRangeRow> {
        conn.prepareStatement(SQL_GET_BOOKS_BY_PRICE_RANGE).use { ps ->
            ps.setBigDecimal(1, price)
            ps.setBigDecimal(2, price2)
            val rows = mutableListOf<GetBooksByPriceRangeRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksByPriceRangeRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    data class GetBooksInGenresRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal
    )

    private val SQL_GET_BOOKS_IN_GENRES = """
        SELECT id, title, genre, price
        FROM book
        WHERE genre IN (?, ?, ?)
        ORDER BY title;
    """.trimIndent()
    fun getBooksInGenres(conn: Connection, genre: String, genre2: String, genre3: String): List<GetBooksInGenresRow> {
        conn.prepareStatement(SQL_GET_BOOKS_IN_GENRES).use { ps ->
            ps.setString(1, genre)
            ps.setString(2, genre2)
            ps.setString(3, genre3)
            val rows = mutableListOf<GetBooksInGenresRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksInGenresRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    data class GetBookPriceLabelRow(
        val id: Long,
        val title: String,
        val price: java.math.BigDecimal,
        val priceLabel: String
    )

    private val SQL_GET_BOOK_PRICE_LABEL = """
        SELECT id, title, price,
               CASE WHEN price > ? THEN 'expensive' ELSE 'affordable' END AS price_label
        FROM book
        ORDER BY title;
    """.trimIndent()
    fun getBookPriceLabel(conn: Connection, price: java.math.BigDecimal): List<GetBookPriceLabelRow> {
        conn.prepareStatement(SQL_GET_BOOK_PRICE_LABEL).use { ps ->
            ps.setBigDecimal(1, price)
            val rows = mutableListOf<GetBookPriceLabelRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBookPriceLabelRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3), rs.getString(4)))
            }
            return rows
        }
    }

    data class GetBookPriceOrDefaultRow(
        val id: Long,
        val title: String,
        val effectivePrice: java.math.BigDecimal
    )

    private val SQL_GET_BOOK_PRICE_OR_DEFAULT = """
        SELECT id, title, COALESCE(price, ?) AS effective_price
        FROM book
        ORDER BY title;
    """.trimIndent()
    fun getBookPriceOrDefault(conn: Connection, price: java.math.BigDecimal?): List<GetBookPriceOrDefaultRow> {
        conn.prepareStatement(SQL_GET_BOOK_PRICE_OR_DEFAULT).use { ps ->
            ps.setObject(1, price)
            val rows = mutableListOf<GetBookPriceOrDefaultRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBookPriceOrDefaultRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3)))
            }
            return rows
        }
    }

    private val SQL_DELETE_BOOK_BY_ID = """
        DELETE FROM book WHERE id = ?;
    """.trimIndent()
    fun deleteBookById(conn: Connection, id: Long): Long {
        conn.prepareStatement(SQL_DELETE_BOOK_BY_ID).use { ps ->
            ps.setLong(1, id)
            return ps.executeUpdate().toLong()
        }
    }

    data class GetGenresWithManyBooksRow(
        val genre: String,
        val bookCount: Long
    )

    private val SQL_GET_GENRES_WITH_MANY_BOOKS = """
        SELECT genre, COUNT(*) AS book_count
        FROM book
        GROUP BY genre
        HAVING COUNT(*) > ?
        ORDER BY genre;
    """.trimIndent()
    fun getGenresWithManyBooks(conn: Connection, count: Long): List<GetGenresWithManyBooksRow> {
        conn.prepareStatement(SQL_GET_GENRES_WITH_MANY_BOOKS).use { ps ->
            ps.setLong(1, count)
            val rows = mutableListOf<GetGenresWithManyBooksRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetGenresWithManyBooksRow(rs.getString(1), rs.getLong(2)))
            }
            return rows
        }
    }

    data class GetBooksByAuthorParamRow(
        val id: Long,
        val title: String,
        val price: java.math.BigDecimal
    )

    private val SQL_GET_BOOKS_BY_AUTHOR_PARAM = """
        SELECT b.id, b.title, b.price
        FROM book b
        JOIN author a ON a.id = b.author_id AND a.birth_year > ?
        ORDER BY b.title;
    """.trimIndent()
    fun getBooksByAuthorParam(conn: Connection, birthYear: Int?): List<GetBooksByAuthorParamRow> {
        conn.prepareStatement(SQL_GET_BOOKS_BY_AUTHOR_PARAM).use { ps ->
            ps.setObject(1, birthYear)
            val rows = mutableListOf<GetBooksByAuthorParamRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksByAuthorParamRow(rs.getLong(1), rs.getString(2), rs.getBigDecimal(3)))
            }
            return rows
        }
    }

    private val SQL_GET_ALL_BOOK_FIELDS = """
        SELECT b.*
        FROM book b
        ORDER BY b.id;
    """.trimIndent()
    fun getAllBookFields(conn: Connection): List<Book> {
        conn.prepareStatement(SQL_GET_ALL_BOOK_FIELDS).use { ps ->
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    data class GetBooksNotByAuthorRow(
        val id: Long,
        val title: String,
        val genre: String
    )

    private val SQL_GET_BOOKS_NOT_BY_AUTHOR = """
        SELECT id, title, genre
        FROM book
        WHERE author_id NOT IN (SELECT id FROM author WHERE name = ?)
        ORDER BY title;
    """.trimIndent()
    fun getBooksNotByAuthor(conn: Connection, name: String): List<GetBooksNotByAuthorRow> {
        conn.prepareStatement(SQL_GET_BOOKS_NOT_BY_AUTHOR).use { ps ->
            ps.setString(1, name)
            val rows = mutableListOf<GetBooksNotByAuthorRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksNotByAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }

    data class GetBooksWithRecentSalesRow(
        val id: Long,
        val title: String,
        val genre: String
    )

    private val SQL_GET_BOOKS_WITH_RECENT_SALES = """
        SELECT id, title, genre
        FROM book
        WHERE EXISTS (
            SELECT 1 FROM sale_item si
            JOIN sale s ON s.id = si.sale_id
            WHERE si.book_id = book.id AND s.ordered_at > ?
        )
        ORDER BY title;
    """.trimIndent()
    fun getBooksWithRecentSales(conn: Connection, orderedAt: java.time.LocalDateTime): List<GetBooksWithRecentSalesRow> {
        conn.prepareStatement(SQL_GET_BOOKS_WITH_RECENT_SALES).use { ps ->
            ps.setObject(1, orderedAt)
            val rows = mutableListOf<GetBooksWithRecentSalesRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksWithRecentSalesRow(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }

    data class GetBookWithAuthorNameRow(
        val id: Long,
        val title: String,
        val authorName: String?
    )

    private val SQL_GET_BOOK_WITH_AUTHOR_NAME = """
        SELECT b.id, b.title,
               (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name
        FROM book b
        ORDER BY b.title;
    """.trimIndent()
    fun getBookWithAuthorName(conn: Connection): List<GetBookWithAuthorNameRow> {
        conn.prepareStatement(SQL_GET_BOOK_WITH_AUTHOR_NAME).use { ps ->
            val rows = mutableListOf<GetBookWithAuthorNameRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBookWithAuthorNameRow(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }

    data class GetAuthorStatsRow(
        val id: Long,
        val name: String,
        val numBooks: Long,
        val totalSold: Long
    )

    private val SQL_GET_AUTHOR_STATS = """
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
        ORDER BY a.name;
    """.trimIndent()
    fun getAuthorStats(conn: Connection): List<GetAuthorStatsRow> {
        conn.prepareStatement(SQL_GET_AUTHOR_STATS).use { ps ->
            val rows = mutableListOf<GetAuthorStatsRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetAuthorStatsRow(rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4)))
            }
            return rows
        }
    }

    data class ArchiveAndReturnBooksRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal
    )

    private val SQL_ARCHIVE_AND_RETURN_BOOKS = """
        WITH archived AS (
            DELETE FROM book
            WHERE published_at < ?
            RETURNING id, title, genre, price
        )
        SELECT id, title, genre, price FROM archived ORDER BY title;
    """.trimIndent()
    fun archiveAndReturnBooks(conn: Connection, publishedAt: java.time.LocalDate?): List<ArchiveAndReturnBooksRow> {
        conn.prepareStatement(SQL_ARCHIVE_AND_RETURN_BOOKS).use { ps ->
            ps.setObject(1, publishedAt)
            val rows = mutableListOf<ArchiveAndReturnBooksRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ArchiveAndReturnBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }

    private val SQL_GET_PRODUCT = """
        SELECT id, sku, name, active, weight_kg, rating, tags, metadata,
               thumbnail, created_at, stock_count
        FROM product
        WHERE id = ?;
    """.trimIndent()
    fun getProduct(conn: Connection, id: java.util.UUID): Product? {
        conn.prepareStatement(SQL_GET_PRODUCT).use { ps ->
            ps.setObject(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Product(rs.getObject(1, java.util.UUID::class.java), rs.getString(2), rs.getString(3), rs.getBoolean(4), getNullableFloat(rs, 5), getNullableDouble(rs, 6), (rs.getArray(7).array as Array<String>).toList(), rs.getString(8), rs.getBytes(9), rs.getObject(10, java.time.LocalDateTime::class.java), rs.getShort(11))
            }
        }
    }

    data class ListActiveProductsRow(
        val id: java.util.UUID,
        val sku: String,
        val name: String,
        val active: Boolean,
        val weightKg: Float?,
        val rating: Double?,
        val tags: List<String>,
        val metadata: String?,
        val createdAt: java.time.LocalDateTime,
        val stockCount: Short
    )

    private val SQL_LIST_ACTIVE_PRODUCTS = """
        SELECT id, sku, name, active, weight_kg, rating, tags, metadata,
               created_at, stock_count
        FROM product
        WHERE active = ?
        ORDER BY name;
    """.trimIndent()
    fun listActiveProducts(conn: Connection, active: Boolean): List<ListActiveProductsRow> {
        conn.prepareStatement(SQL_LIST_ACTIVE_PRODUCTS).use { ps ->
            ps.setBoolean(1, active)
            val rows = mutableListOf<ListActiveProductsRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListActiveProductsRow(rs.getObject(1, java.util.UUID::class.java), rs.getString(2), rs.getString(3), rs.getBoolean(4), getNullableFloat(rs, 5), getNullableDouble(rs, 6), (rs.getArray(7).array as Array<String>).toList(), rs.getString(8), rs.getObject(9, java.time.LocalDateTime::class.java), rs.getShort(10)))
            }
            return rows
        }
    }

    private val SQL_INSERT_PRODUCT = """
        INSERT INTO product (id, sku, name, active, weight_kg, rating, tags, metadata, thumbnail, stock_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun insertProduct(conn: Connection, id: java.util.UUID, sku: String, name: String, active: Boolean, weightKg: Float?, rating: Double?, tags: List<String>, metadata: String?, thumbnail: ByteArray?, stockCount: Short): Product? {
        conn.prepareStatement(SQL_INSERT_PRODUCT).use { ps ->
            ps.setObject(1, id)
            ps.setString(2, sku)
            ps.setString(3, name)
            ps.setBoolean(4, active)
            ps.setObject(5, weightKg)
            ps.setObject(6, rating)
            ps.setArray(7, conn.createArrayOf("text", tags.toTypedArray()))
            ps.setObject(8, metadata, java.sql.Types.OTHER)
            ps.setObject(9, thumbnail)
            ps.setShort(10, stockCount)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Product(rs.getObject(1, java.util.UUID::class.java), rs.getString(2), rs.getString(3), rs.getBoolean(4), getNullableFloat(rs, 5), getNullableDouble(rs, 6), (rs.getArray(7).array as Array<String>).toList(), rs.getString(8), rs.getBytes(9), rs.getObject(10, java.time.LocalDateTime::class.java), rs.getShort(11))
            }
        }
    }

    data class GetAuthorsWithNullBioRow(
        val id: Long,
        val name: String,
        val birthYear: Int?
    )

    private val SQL_GET_AUTHORS_WITH_NULL_BIO = """
        SELECT id, name, birth_year
        FROM author
        WHERE bio IS NULL
        ORDER BY name;
    """.trimIndent()
    fun getAuthorsWithNullBio(conn: Connection): List<GetAuthorsWithNullBioRow> {
        conn.prepareStatement(SQL_GET_AUTHORS_WITH_NULL_BIO).use { ps ->
            val rows = mutableListOf<GetAuthorsWithNullBioRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetAuthorsWithNullBioRow(rs.getLong(1), rs.getString(2), getNullableInt(rs, 3)))
            }
            return rows
        }
    }

    private val SQL_GET_AUTHORS_WITH_BIO = """
        SELECT id, name, bio, birth_year
        FROM author
        WHERE bio IS NOT NULL
        ORDER BY name;
    """.trimIndent()
    fun getAuthorsWithBio(conn: Connection): List<Author> {
        conn.prepareStatement(SQL_GET_AUTHORS_WITH_BIO).use { ps ->
            val rows = mutableListOf<Author>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4)))
            }
            return rows
        }
    }

    data class GetBooksPublishedBetweenRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal,
        val publishedAt: java.time.LocalDate?
    )

    private val SQL_GET_BOOKS_PUBLISHED_BETWEEN = """
        SELECT id, title, genre, price, published_at
        FROM book
        WHERE published_at IS NOT NULL
          AND published_at BETWEEN ? AND ?
        ORDER BY published_at;
    """.trimIndent()
    fun getBooksPublishedBetween(conn: Connection, publishedAt: java.time.LocalDate?, publishedAt2: java.time.LocalDate?): List<GetBooksPublishedBetweenRow> {
        conn.prepareStatement(SQL_GET_BOOKS_PUBLISHED_BETWEEN).use { ps ->
            ps.setObject(1, publishedAt)
            ps.setObject(2, publishedAt2)
            val rows = mutableListOf<GetBooksPublishedBetweenRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksPublishedBetweenRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5, java.time.LocalDate::class.java)))
            }
            return rows
        }
    }

    data class GetDistinctGenresRow(
        val genre: String
    )

    private val SQL_GET_DISTINCT_GENRES = """
        SELECT DISTINCT genre
        FROM book
        ORDER BY genre;
    """.trimIndent()
    fun getDistinctGenres(conn: Connection): List<GetDistinctGenresRow> {
        conn.prepareStatement(SQL_GET_DISTINCT_GENRES).use { ps ->
            val rows = mutableListOf<GetDistinctGenresRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetDistinctGenresRow(rs.getString(1)))
            }
            return rows
        }
    }

    data class GetBooksWithSalesCountRow(
        val id: Long,
        val title: String,
        val genre: String,
        val totalQuantity: Long
    )

    private val SQL_GET_BOOKS_WITH_SALES_COUNT = """
        SELECT b.id, b.title, b.genre,
               COALESCE(SUM(si.quantity), 0) AS total_quantity
        FROM book b
        LEFT JOIN sale_item si ON si.book_id = b.id
        GROUP BY b.id, b.title, b.genre
        ORDER BY total_quantity DESC, b.title;
    """.trimIndent()
    fun getBooksWithSalesCount(conn: Connection): List<GetBooksWithSalesCountRow> {
        conn.prepareStatement(SQL_GET_BOOKS_WITH_SALES_COUNT).use { ps ->
            val rows = mutableListOf<GetBooksWithSalesCountRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBooksWithSalesCountRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4)))
            }
            return rows
        }
    }

    data class CountSaleItemsRow(
        val itemCount: Long
    )

    private val SQL_COUNT_SALE_ITEMS = """
        SELECT COUNT(*) AS item_count
        FROM sale_item
        WHERE sale_id = ?;
    """.trimIndent()
    fun countSaleItems(conn: Connection, saleId: Long): CountSaleItemsRow? {
        conn.prepareStatement(SQL_COUNT_SALE_ITEMS).use { ps ->
            ps.setLong(1, saleId)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return CountSaleItemsRow(rs.getLong(1))
            }
        }
    }

    data class UpsertProductRow(
        val id: java.util.UUID,
        val sku: String,
        val name: String,
        val active: Boolean,
        val tags: List<String>,
        val stockCount: Short
    )

    private val SQL_UPSERT_PRODUCT = """
        INSERT INTO product (id, sku, name, active, tags, stock_count)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE
            SET name        = EXCLUDED.name,
                active      = EXCLUDED.active,
                tags        = EXCLUDED.tags,
                stock_count = EXCLUDED.stock_count
        RETURNING id, sku, name, active, tags, stock_count;
    """.trimIndent()
    fun upsertProduct(conn: Connection, id: java.util.UUID, sku: String, name: String, active: Boolean, tags: List<String>, stockCount: Short): UpsertProductRow? {
        conn.prepareStatement(SQL_UPSERT_PRODUCT).use { ps ->
            ps.setObject(1, id)
            ps.setString(2, sku)
            ps.setString(3, name)
            ps.setBoolean(4, active)
            ps.setArray(5, conn.createArrayOf("text", tags.toTypedArray()))
            ps.setShort(6, stockCount)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return UpsertProductRow(rs.getObject(1, java.util.UUID::class.java), rs.getString(2), rs.getString(3), rs.getBoolean(4), (rs.getArray(5).array as Array<String>).toList(), rs.getShort(6))
            }
        }
    }

    data class GetSaleItemQuantityAggregatesRow(
        val minQty: Int?,
        val maxQty: Int?,
        val sumQty: Long?,
        val avgQty: java.math.BigDecimal?
    )

    private val SQL_GET_SALE_ITEM_QUANTITY_AGGREGATES = """
        SELECT MIN(quantity)  AS min_qty,
               MAX(quantity)  AS max_qty,
               SUM(quantity)  AS sum_qty,
               AVG(quantity)  AS avg_qty
        FROM sale_item;
    """.trimIndent()
    fun getSaleItemQuantityAggregates(conn: Connection): GetSaleItemQuantityAggregatesRow? {
        conn.prepareStatement(SQL_GET_SALE_ITEM_QUANTITY_AGGREGATES).use { ps ->
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return GetSaleItemQuantityAggregatesRow(getNullableInt(rs, 1), getNullableInt(rs, 2), getNullableLong(rs, 3), rs.getBigDecimal(4))
            }
        }
    }

    data class GetBookPriceAggregatesRow(
        val minPrice: java.math.BigDecimal?,
        val maxPrice: java.math.BigDecimal?,
        val sumPrice: java.math.BigDecimal?,
        val avgPrice: java.math.BigDecimal?
    )

    private val SQL_GET_BOOK_PRICE_AGGREGATES = """
        SELECT MIN(price)  AS min_price,
               MAX(price)  AS max_price,
               SUM(price)  AS sum_price,
               AVG(price)  AS avg_price
        FROM book;
    """.trimIndent()
    fun getBookPriceAggregates(conn: Connection): GetBookPriceAggregatesRow? {
        conn.prepareStatement(SQL_GET_BOOK_PRICE_AGGREGATES).use { ps ->
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return GetBookPriceAggregatesRow(rs.getBigDecimal(1), rs.getBigDecimal(2), rs.getBigDecimal(3), rs.getBigDecimal(4))
            }
        }
    }

    private fun getNullableBoolean(rs: java.sql.ResultSet, col: Int): Boolean? {
        val v = rs.getBoolean(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableShort(rs: java.sql.ResultSet, col: Int): Short? {
        val v = rs.getShort(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableInt(rs: java.sql.ResultSet, col: Int): Int? {
        val v = rs.getInt(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableLong(rs: java.sql.ResultSet, col: Int): Long? {
        val v = rs.getLong(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableFloat(rs: java.sql.ResultSet, col: Int): Float? {
        val v = rs.getFloat(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableDouble(rs: java.sql.ResultSet, col: Int): Double? {
        val v = rs.getDouble(col)
        return if (rs.wasNull()) null else v
    }
}
