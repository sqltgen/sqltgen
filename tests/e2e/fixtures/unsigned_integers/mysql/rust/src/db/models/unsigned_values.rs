#[derive(Debug, sqlx::FromRow)]
pub struct UnsignedValues {
    pub id: u64,
    pub u8_val: u8,
    pub u16_val: u16,
    pub u24_val: u32,
    pub u32_val: u32,
    pub u64_val: u64,
}
