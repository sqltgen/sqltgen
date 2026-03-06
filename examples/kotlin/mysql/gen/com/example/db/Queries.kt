package com.example.db

import java.sql.Connection

object Queries {

    private const val SQL_CREATE_AUTHOR = "INSERT INTO author (name, bio, birth_year) VALUES (?, ?, ?);"
    fun createAuthor(conn: Connection, name: String, bio: String?, birthYear: Int?): Unit {
        conn.prepareStatement(SQL_CREATE_AUTHOR).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, bio)
            ps.setObject(3, birthYear)
            ps.executeUpdate()
        }
    }

    private const val SQL_GET_AUTHOR = "SELECT id, name, bio, birth_year FROM author WHERE id = ?;"
    fun getAuthor(conn: Connection, id: Long): Author? {
        conn.prepareStatement(SQL_GET_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, java.lang.Integer::class.java)?.toInt())
            }
        }
    }

    private const val SQL_LIST_AUTHORS = "SELECT id, name, bio, birth_year FROM author ORDER BY name;"
    fun listAuthors(conn: Connection): List<Author> {
        conn.prepareStatement(SQL_LIST_AUTHORS).use { ps ->
            val rows = mutableListOf<Author>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4, java.lang.Integer::class.java)?.toInt()))
            }
            return rows
        }
    }

    private const val SQL_UPDATE_AUTHOR_BIO = "UPDATE author SET bio = ? WHERE id = ?;"
    fun updateAuthorBio(conn: Connection, bio: String?, id: Long): Unit {
        conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO).use { ps ->
            ps.setObject(1, bio)
            ps.setLong(2, id)
            ps.executeUpdate()
        }
    }

    private const val SQL_DELETE_AUTHOR = "DELETE FROM author WHERE id = ?;"
    fun deleteAuthor(conn: Connection, id: Long): Unit {
        conn.prepareStatement(SQL_DELETE_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeUpdate()
        }
    }

    private const val SQL_CREATE_BOOK = "INSERT INTO book (author_id, title, genre, price, published_at) VALUES (?, ?, ?, ?, ?);"
    fun createBook(conn: Connection, authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Unit {
        conn.prepareStatement(SQL_CREATE_BOOK).use { ps ->
            ps.setLong(1, authorId)
            ps.setString(2, title)
            ps.setString(3, genre)
            ps.setBigDecimal(4, price)
            ps.setObject(5, publishedAt)
            ps.executeUpdate()
        }
    }

    private const val SQL_GET_BOOK = "SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ?;"
    fun getBook(conn: Connection, id: Long): Book? {
        conn.prepareStatement(SQL_GET_BOOK).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java))
            }
        }
    }

    private const val SQL_LIST_BOOKS_BY_GENRE = "SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = ? ORDER BY title;"
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

    private const val SQL_CREATE_CUSTOMER = "INSERT INTO customer (name, email) VALUES (?, ?);"
    fun createCustomer(conn: Connection, name: String, email: String): Unit {
        conn.prepareStatement(SQL_CREATE_CUSTOMER).use { ps ->
            ps.setString(1, name)
            ps.setString(2, email)
            ps.executeUpdate()
        }
    }

    private const val SQL_CREATE_SALE = "INSERT INTO sale (customer_id) VALUES (?);"
    fun createSale(conn: Connection, customerId: Long): Unit {
        conn.prepareStatement(SQL_CREATE_SALE).use { ps ->
            ps.setLong(1, customerId)
            ps.executeUpdate()
        }
    }

    private const val SQL_ADD_SALE_ITEM = "INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?, ?, ?, ?);"
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

    private const val SQL_LIST_BOOKS_WITH_AUTHOR = "SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title;"
    fun listBooksWithAuthor(conn: Connection): List<ListBooksWithAuthorRow> {
        conn.prepareStatement(SQL_LIST_BOOKS_WITH_AUTHOR).use { ps ->
            val rows = mutableListOf<ListBooksWithAuthorRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListBooksWithAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5, java.time.LocalDate::class.java), rs.getString(6), rs.getString(7)))
            }
            return rows
        }
    }

    private const val SQL_GET_BOOKS_NEVER_ORDERED = "SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title;"
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
        val unitsSold: java.math.BigDecimal?
    )

    private const val SQL_GET_TOP_SELLING_BOOKS = "WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC;"
    fun getTopSellingBooks(conn: Connection): List<GetTopSellingBooksRow> {
        conn.prepareStatement(SQL_GET_TOP_SELLING_BOOKS).use { ps ->
            val rows = mutableListOf<GetTopSellingBooksRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetTopSellingBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getBigDecimal(5)))
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

    private const val SQL_GET_BEST_CUSTOMERS = "WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC;"
    fun getBestCustomers(conn: Connection): List<GetBestCustomersRow> {
        conn.prepareStatement(SQL_GET_BEST_CUSTOMERS).use { ps ->
            val rows = mutableListOf<GetBestCustomersRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBestCustomersRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4)))
            }
            return rows
        }
    }
}
