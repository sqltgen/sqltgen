package db.models

data class SaleItem(
    val id: Long,
    val saleId: Long,
    val bookId: Long,
    val quantity: Int,
    val unitPrice: java.math.BigDecimal
)
