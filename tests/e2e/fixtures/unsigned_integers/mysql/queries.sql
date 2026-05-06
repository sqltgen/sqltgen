-- name: InsertUnsignedRow :exec
INSERT INTO unsigned_values (u8_val, u16_val, u24_val, u32_val, u64_val)
VALUES ($1, $2, $3, $4, $5);

-- name: GetUnsignedRows :many
SELECT id, u8_val, u16_val, u24_val, u32_val, u64_val
FROM unsigned_values
ORDER BY id;
