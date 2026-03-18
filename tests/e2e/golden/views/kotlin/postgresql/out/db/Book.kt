package db

data class Book(
    val id: Long,
    val authorId: Long,
    val title: String,
    val genre: String
)
