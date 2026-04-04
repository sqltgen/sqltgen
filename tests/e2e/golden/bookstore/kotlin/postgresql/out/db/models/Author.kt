package db.models

data class Author(
    val id: Long,
    val name: String,
    val bio: String?,
    val birthYear: Int?
)
