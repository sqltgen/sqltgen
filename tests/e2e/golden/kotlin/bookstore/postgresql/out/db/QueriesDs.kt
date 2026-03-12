package db

import javax.sql.DataSource

class QueriesDs(private val dataSource: DataSource) {

    fun createAuthor(name: String, bio: String?, birthYear: Int?): Author? =
        dataSource.connection.use { conn -> Queries.createAuthor(conn, name, bio, birthYear) }

    fun getAuthor(id: Long): Author? =
        dataSource.connection.use { conn -> Queries.getAuthor(conn, id) }

    fun listAuthors(): List<Author> =
        dataSource.connection.use { conn -> Queries.listAuthors(conn) }

    fun updateAuthorBio(bio: String?, id: Long): Author? =
        dataSource.connection.use { conn -> Queries.updateAuthorBio(conn, bio, id) }

    fun deleteAuthor(id: Long): Queries.DeleteAuthorRow? =
        dataSource.connection.use { conn -> Queries.deleteAuthor(conn, id) }

    fun createBook(authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Book? =
        dataSource.connection.use { conn -> Queries.createBook(conn, authorId, title, genre, price, publishedAt) }

    fun getBook(id: Long): Book? =
        dataSource.connection.use { conn -> Queries.getBook(conn, id) }

    fun getBooksByIds(ids: List<Long>): List<Book> =
        dataSource.connection.use { conn -> Queries.getBooksByIds(conn, ids) }

    fun listBooksByGenre(genre: String): List<Book> =
        dataSource.connection.use { conn -> Queries.listBooksByGenre(conn, genre) }

    fun listBooksByGenreOrAll(genre: String): List<Book> =
        dataSource.connection.use { conn -> Queries.listBooksByGenreOrAll(conn, genre) }

    fun createCustomer(name: String, email: String): Queries.CreateCustomerRow? =
        dataSource.connection.use { conn -> Queries.createCustomer(conn, name, email) }

    fun createSale(customerId: Long): Queries.CreateSaleRow? =
        dataSource.connection.use { conn -> Queries.createSale(conn, customerId) }

    fun addSaleItem(saleId: Long, bookId: Long, quantity: Int, unitPrice: java.math.BigDecimal): Unit =
        dataSource.connection.use { conn -> Queries.addSaleItem(conn, saleId, bookId, quantity, unitPrice) }

    fun listBooksWithAuthor(): List<Queries.ListBooksWithAuthorRow> =
        dataSource.connection.use { conn -> Queries.listBooksWithAuthor(conn) }

    fun getBooksNeverOrdered(): List<Book> =
        dataSource.connection.use { conn -> Queries.getBooksNeverOrdered(conn) }

    fun getTopSellingBooks(): List<Queries.GetTopSellingBooksRow> =
        dataSource.connection.use { conn -> Queries.getTopSellingBooks(conn) }

    fun getBestCustomers(): List<Queries.GetBestCustomersRow> =
        dataSource.connection.use { conn -> Queries.getBestCustomers(conn) }

    fun countBooksByGenre(): List<Queries.CountBooksByGenreRow> =
        dataSource.connection.use { conn -> Queries.countBooksByGenre(conn) }

    fun listBooksWithLimit(limit: Long, offset: Long): List<Queries.ListBooksWithLimitRow> =
        dataSource.connection.use { conn -> Queries.listBooksWithLimit(conn, limit, offset) }

    fun searchBooksByTitle(title: String): List<Queries.SearchBooksByTitleRow> =
        dataSource.connection.use { conn -> Queries.searchBooksByTitle(conn, title) }

    fun getBooksByPriceRange(price: java.math.BigDecimal, price2: java.math.BigDecimal): List<Queries.GetBooksByPriceRangeRow> =
        dataSource.connection.use { conn -> Queries.getBooksByPriceRange(conn, price, price2) }

    fun getBooksInGenres(genre: String, genre2: String, genre3: String): List<Queries.GetBooksInGenresRow> =
        dataSource.connection.use { conn -> Queries.getBooksInGenres(conn, genre, genre2, genre3) }

    fun getBookPriceLabel(price: java.math.BigDecimal): List<Queries.GetBookPriceLabelRow> =
        dataSource.connection.use { conn -> Queries.getBookPriceLabel(conn, price) }

    fun getBookPriceOrDefault(price: java.math.BigDecimal?): List<Queries.GetBookPriceOrDefaultRow> =
        dataSource.connection.use { conn -> Queries.getBookPriceOrDefault(conn, price) }

    fun deleteBookById(id: Long): Long =
        dataSource.connection.use { conn -> Queries.deleteBookById(conn, id) }

    fun getGenresWithManyBooks(count: Long): List<Queries.GetGenresWithManyBooksRow> =
        dataSource.connection.use { conn -> Queries.getGenresWithManyBooks(conn, count) }

    fun getBooksByAuthorParam(birthYear: Int?): List<Queries.GetBooksByAuthorParamRow> =
        dataSource.connection.use { conn -> Queries.getBooksByAuthorParam(conn, birthYear) }

    fun getAllBookFields(): List<Book> =
        dataSource.connection.use { conn -> Queries.getAllBookFields(conn) }

    fun getBooksNotByAuthor(name: String): List<Queries.GetBooksNotByAuthorRow> =
        dataSource.connection.use { conn -> Queries.getBooksNotByAuthor(conn, name) }

    fun getBooksWithRecentSales(orderedAt: java.time.LocalDateTime): List<Queries.GetBooksWithRecentSalesRow> =
        dataSource.connection.use { conn -> Queries.getBooksWithRecentSales(conn, orderedAt) }

    fun getBookWithAuthorName(): List<Queries.GetBookWithAuthorNameRow> =
        dataSource.connection.use { conn -> Queries.getBookWithAuthorName(conn) }

    fun getAuthorStats(): List<Queries.GetAuthorStatsRow> =
        dataSource.connection.use { conn -> Queries.getAuthorStats(conn) }

    fun archiveAndReturnBooks(publishedAt: java.time.LocalDate?): List<Queries.ArchiveAndReturnBooksRow> =
        dataSource.connection.use { conn -> Queries.archiveAndReturnBooks(conn, publishedAt) }

    fun getProduct(id: java.util.UUID): Product? =
        dataSource.connection.use { conn -> Queries.getProduct(conn, id) }

    fun listActiveProducts(active: Boolean): List<Queries.ListActiveProductsRow> =
        dataSource.connection.use { conn -> Queries.listActiveProducts(conn, active) }

    fun insertProduct(id: java.util.UUID, sku: String, name: String, active: Boolean, weightKg: Float?, rating: Double?, tags: List<String>, metadata: String?, thumbnail: ByteArray?, stockCount: Short): Product? =
        dataSource.connection.use { conn -> Queries.insertProduct(conn, id, sku, name, active, weightKg, rating, tags, metadata, thumbnail, stockCount) }

    fun getAuthorsWithNullBio(): List<Queries.GetAuthorsWithNullBioRow> =
        dataSource.connection.use { conn -> Queries.getAuthorsWithNullBio(conn) }

    fun getAuthorsWithBio(): List<Author> =
        dataSource.connection.use { conn -> Queries.getAuthorsWithBio(conn) }

    fun getBooksPublishedBetween(publishedAt: java.time.LocalDate?, publishedAt2: java.time.LocalDate?): List<Queries.GetBooksPublishedBetweenRow> =
        dataSource.connection.use { conn -> Queries.getBooksPublishedBetween(conn, publishedAt, publishedAt2) }

    fun getDistinctGenres(): List<Queries.GetDistinctGenresRow> =
        dataSource.connection.use { conn -> Queries.getDistinctGenres(conn) }

    fun getBooksWithSalesCount(): List<Queries.GetBooksWithSalesCountRow> =
        dataSource.connection.use { conn -> Queries.getBooksWithSalesCount(conn) }

    fun countSaleItems(saleId: Long): Queries.CountSaleItemsRow? =
        dataSource.connection.use { conn -> Queries.countSaleItems(conn, saleId) }

    fun upsertProduct(id: java.util.UUID, sku: String, name: String, active: Boolean, tags: List<String>, stockCount: Short): Queries.UpsertProductRow? =
        dataSource.connection.use { conn -> Queries.upsertProduct(conn, id, sku, name, active, tags, stockCount) }

    fun getSaleItemQuantityAggregates(): Queries.GetSaleItemQuantityAggregatesRow? =
        dataSource.connection.use { conn -> Queries.getSaleItemQuantityAggregates(conn) }

    fun getBookPriceAggregates(): Queries.GetBookPriceAggregatesRow? =
        dataSource.connection.use { conn -> Queries.getBookPriceAggregates(conn) }
}
