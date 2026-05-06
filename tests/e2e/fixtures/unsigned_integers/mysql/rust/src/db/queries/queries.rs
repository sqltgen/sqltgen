use super::super::sqltgen::DbPool;

use super::super::models::unsigned_values::UnsignedValues;

pub async fn insert_unsigned_row(pool: &DbPool, u8_val: u8, u16_val: u16, u24_val: u32, u32_val: u32, u64_val: u64) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO unsigned_values (u8_val, u16_val, u24_val, u32_val, u64_val)
        VALUES (?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(u8_val)
        .bind(u16_val)
        .bind(u24_val)
        .bind(u32_val)
        .bind(u64_val)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_unsigned_rows(pool: &DbPool) -> Result<Vec<UnsignedValues>, sqlx::Error> {
    let sql = r##"
        SELECT id, u8_val, u16_val, u24_val, u32_val, u64_val
        FROM unsigned_values
        ORDER BY id
    "##;
    sqlx::query_as::<_, UnsignedValues>(sql)
        .fetch_all(pool)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn insert_unsigned_row(&self, u8_val: u8, u16_val: u16, u24_val: u32, u32_val: u32, u64_val: u64) -> Result<(), sqlx::Error> {
        insert_unsigned_row(self.pool, u8_val, u16_val, u24_val, u32_val, u64_val).await
    }

    pub async fn get_unsigned_rows(&self) -> Result<Vec<UnsignedValues>, sqlx::Error> {
        get_unsigned_rows(self.pool).await
    }
}
