package com.example.db.pg

import java.sql.Connection

object Queries {

    private const val SQL_CREATEAUTHOR = "INSERT INTO author (name, bio, birth_year) VALUES (?, ?, ?);"
    fun createAuthor(conn: Connection, name: String, bio: String?, birthYear: Int?): Unit {
        conn.prepareStatement(SQL_CREATEAUTHOR).use { ps ->
            ps.setString(1, name)
            ps.setString(2, bio)
            ps.setInt(3, birthYear)
            ps.executeUpdate()
        }
    }

    private const val SQL_GETAUTHOR = "SELECT id, name, bio, birth_year FROM author WHERE id = ?;"
    fun getAuthor(conn: Connection, id: Long): Author? {
        conn.prepareStatement(SQL_GETAUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4))
            }
        }
    }

    private const val SQL_LISTAUTHORS = "SELECT id, name, bio, birth_year FROM author ORDER BY name;"
    fun listAuthors(conn: Connection): List<Author> {
        conn.prepareStatement(SQL_LISTAUTHORS).use { ps ->
            val rows = mutableListOf<Author>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Author(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4)))
            }
            return rows
        }
    }

    private const val SQL_CREATEBOOK = "INSERT INTO book (author_id, title, genre, price, published_at) VALUES (?, ?, ?, ?, ?);"
    fun createBook(conn: Connection, authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Unit {
        conn.prepareStatement(SQL_CREATEBOOK).use { ps ->
            ps.setLong(1, authorId)
            ps.setString(2, title)
            ps.setString(3, genre)
            ps.setBigDecimal(4, price)
            ps.setObject(5, publishedAt)
            ps.executeUpdate()
        }
    }

    private const val SQL_GETBOOK = "SELECT id, author_id, title, genre, price, published_at FROM book WHERE id = ?;"
    fun getBook(conn: Connection, id: Long): Book? {
        conn.prepareStatement(SQL_GETBOOK).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6))
            }
        }
    }

    private const val SQL_LISTBOOKSBYGENRE = "SELECT id, author_id, title, genre, price, published_at FROM book WHERE genre = ? ORDER BY title;"
    fun listBooksByGenre(conn: Connection, genre: String): List<Book> {
        conn.prepareStatement(SQL_LISTBOOKSBYGENRE).use { ps ->
            ps.setString(1, genre)
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6)))
            }
            return rows
        }
    }

    private const val SQL_CREATECUSTOMER = "INSERT INTO customer (name, email) VALUES (?, ?);"
    fun createCustomer(conn: Connection, name: String, email: String): Unit {
        conn.prepareStatement(SQL_CREATECUSTOMER).use { ps ->
            ps.setString(1, name)
            ps.setString(2, email)
            ps.executeUpdate()
        }
    }

    private const val SQL_CREATESALE = "INSERT INTO sale (customer_id) VALUES (?);"
    fun createSale(conn: Connection, customerId: Long): Unit {
        conn.prepareStatement(SQL_CREATESALE).use { ps ->
            ps.setLong(1, customerId)
            ps.executeUpdate()
        }
    }

    private const val SQL_ADDSALEITEM = "INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?, ?, ?, ?);"
    fun addSaleItem(conn: Connection, saleId: Long, bookId: Long, quantity: Int, unitPrice: java.math.BigDecimal): Unit {
        conn.prepareStatement(SQL_ADDSALEITEM).use { ps ->
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

    private const val SQL_LISTBOOKSWITHAUTHOR = "SELECT b.id, b.title, b.genre, b.price, b.published_at,        a.name AS author_name, a.bio AS author_bio FROM book b JOIN author a ON a.id = b.author_id ORDER BY b.title;"
    fun listBooksWithAuthor(conn: Connection): List<ListBooksWithAuthorRow> {
        conn.prepareStatement(SQL_LISTBOOKSWITHAUTHOR).use { ps ->
            val rows = mutableListOf<ListBooksWithAuthorRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(ListBooksWithAuthorRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5), rs.getString(6), rs.getString(7)))
            }
            return rows
        }
    }

    private const val SQL_GETBOOKSNEVERORDERED = "SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at FROM book b LEFT JOIN sale_item si ON si.book_id = b.id WHERE si.id IS NULL ORDER BY b.title;"
    fun getBooksNeverOrdered(conn: Connection): List<Book> {
        conn.prepareStatement(SQL_GETBOOKSNEVERORDERED).use { ps ->
            val rows = mutableListOf<Book>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6)))
            }
            return rows
        }
    }

    data class GetTopSellingBooksRow(
        val id: Long,
        val title: String,
        val genre: String,
        val price: java.math.BigDecimal,
        val unitsSold: Any?
    )

    private const val SQL_GETTOPSELLINGBOOKS = "WITH book_sales AS (     SELECT book_id,            SUM(quantity) AS units_sold     FROM sale_item     GROUP BY book_id ) SELECT b.id, b.title, b.genre, b.price,        bs.units_sold FROM book b JOIN book_sales bs ON bs.book_id = b.id ORDER BY bs.units_sold DESC;"
    fun getTopSellingBooks(conn: Connection): List<GetTopSellingBooksRow> {
        conn.prepareStatement(SQL_GETTOPSELLINGBOOKS).use { ps ->
            val rows = mutableListOf<GetTopSellingBooksRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetTopSellingBooksRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBigDecimal(4), rs.getObject(5)))
            }
            return rows
        }
    }

    data class GetBestCustomersRow(
        val id: Long,
        val name: String,
        val email: String,
        val totalSpent: Any?
    )

    private const val SQL_GETBESTCUSTOMERS = "WITH customer_spend AS (     SELECT s.customer_id,            SUM(si.quantity * si.unit_price) AS total_spent     FROM sale s     JOIN sale_item si ON si.sale_id = s.id     GROUP BY s.customer_id ) SELECT c.id, c.name, c.email,        cs.total_spent FROM customer c JOIN customer_spend cs ON cs.customer_id = c.id ORDER BY cs.total_spent DESC;"
    fun getBestCustomers(conn: Connection): List<GetBestCustomersRow> {
        conn.prepareStatement(SQL_GETBESTCUSTOMERS).use { ps ->
            val rows = mutableListOf<GetBestCustomersRow>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(GetBestCustomersRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getObject(4)))
            }
            return rows
        }
    }
}
