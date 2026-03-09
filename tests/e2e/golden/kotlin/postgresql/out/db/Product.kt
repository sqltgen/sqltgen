package db

data class Product(
    val id: java.util.UUID,
    val sku: String,
    val name: String,
    val active: Boolean,
    val weightKg: Float?,
    val rating: Double?,
    val tags: List<String>,
    val metadata: String?,
    val thumbnail: ByteArray?,
    val createdAt: java.time.LocalDateTime,
    val stockCount: Short
)
