package db

data class Book(
    val id: Long,
    val authorId: Long,
    val title: String,
    val genre: String,
    val price: java.math.BigDecimal,
    val publishedAt: java.time.LocalDate?
)
