use super::*;

// Rules:
//   MIN / MAX  — always same type as argument, always nullable (no rows case)
//   SUM        — preserves type, except integer inputs are widened to avoid
//                overflow (SmallInt/Integer → sum_integer_type, BigInt →
//                sum_bigint_type; defaults: BigInt and BigInt for SQLite/PG
//                default, Decimal for MySQL/PG bigint)
//   AVG        — preserves type for float/decimal inputs; integer/bigint
//                inputs are widened to a fractional type (avg_integer_type:
//                Decimal for PG, Double for MySQL/SQLite)
//
// The test helpers use explicit ResolverConfig values to document the
// per-dialect behaviour without depending on the dialect parse functions.

// ── MIN / MAX ─────────────────────────────────────────────────────────────

#[test]
fn test_aggregate_min_max_preserve_type_and_are_nullable() {
    let schema = make_numeric_schema();
    for config in [pg_config(), mysql_config(), sqlite_config()] {
        let (t, n) = agg_col("MIN(int_val) AS mn", &schema, &config, "mn");
        assert_eq!(t, SqlType::Integer, "MIN(integer) must stay Integer");
        assert!(n, "MIN result is always nullable (no rows)");

        let (t, _) = agg_col("MAX(dec_val) AS mx", &schema, &config, "mx");
        assert_eq!(t, SqlType::Decimal, "MAX(decimal) must stay Decimal");

        let (t, _) = agg_col("MIN(label) AS ml", &schema, &config, "ml");
        assert_eq!(t, SqlType::Text, "MIN(text) must stay Text");

        let (t, _) = agg_col("MAX(dbl_val) AS md", &schema, &config, "md");
        assert_eq!(t, SqlType::Double, "MAX(double) must stay Double");
    }
}

// ── SUM ───────────────────────────────────────────────────────────────────

#[test]
fn test_aggregate_sum_smallint_integer_widened_per_dialect() {
    let schema = make_numeric_schema();

    let (t, _) = agg_col("SUM(int_val) AS s", &schema, &pg_config(), "s");
    assert_eq!(t, SqlType::BigInt, "PG SUM(integer) → bigint");

    let (t, _) = agg_col("SUM(int_val) AS s", &schema, &mysql_config(), "s");
    assert_eq!(t, SqlType::Decimal, "MySQL SUM(integer) → decimal");

    let (t, _) = agg_col("SUM(int_val) AS s", &schema, &sqlite_config(), "s");
    assert_eq!(t, SqlType::BigInt, "SQLite SUM(integer) → bigint");

    let (t, _) = agg_col("SUM(small_val) AS s", &schema, &pg_config(), "s");
    assert_eq!(t, SqlType::BigInt, "PG SUM(smallint) → bigint");
}

#[test]
fn test_aggregate_sum_bigint_widened_per_dialect() {
    // PostgreSQL and MySQL widen SUM(bigint) to numeric/decimal to prevent overflow.
    // SQLite keeps it as BigInt (SQLite's integer is arbitrary precision).
    let schema = make_numeric_schema();

    let (t, _) = agg_col("SUM(big_val) AS s", &schema, &pg_config(), "s");
    assert_eq!(t, SqlType::Decimal, "PG SUM(bigint) → numeric (Decimal)");

    let (t, _) = agg_col("SUM(big_val) AS s", &schema, &mysql_config(), "s");
    assert_eq!(t, SqlType::Decimal, "MySQL SUM(bigint) → decimal");

    let (t, _) = agg_col("SUM(big_val) AS s", &schema, &sqlite_config(), "s");
    assert_eq!(t, SqlType::BigInt, "SQLite SUM(bigint) → bigint (no overflow concern)");
}

#[test]
fn test_aggregate_sum_decimal_and_double_preserved() {
    let schema = make_numeric_schema();
    for config in [pg_config(), mysql_config(), sqlite_config()] {
        let (t, n) = agg_col("SUM(dec_val) AS s", &schema, &config, "s");
        assert_eq!(t, SqlType::Decimal, "SUM(decimal) must stay Decimal in all dialects");
        assert!(n, "SUM result is always nullable");

        let (t, _) = agg_col("SUM(dbl_val) AS s", &schema, &config, "s");
        assert_eq!(t, SqlType::Double, "SUM(double) must stay Double in all dialects");
    }
}

#[test]
fn test_aggregate_sum_expression_uses_expression_type() {
    // SUM(int_val * dec_val): arithmetic widens int*decimal → Decimal,
    // and SUM(Decimal) stays Decimal (no extra widening for non-integers).
    let schema = make_numeric_schema();
    for config in [pg_config(), mysql_config(), sqlite_config()] {
        let (t, _) = agg_col("SUM(int_val * dec_val) AS s", &schema, &config, "s");
        assert_eq!(t, SqlType::Decimal, "SUM(int * decimal) → Decimal via arithmetic widening");
    }
}

// ── AVG ───────────────────────────────────────────────────────────────────

#[test]
fn test_aggregate_avg_integer_widened_to_fractional_per_dialect() {
    // AVG of integers always produces a fractional result, so the integer
    // type is widened. PG and MySQL return DECIMAL on the wire; SQLite returns REAL.
    let schema = make_numeric_schema();

    for expr in ["AVG(int_val) AS a", "AVG(big_val) AS a", "AVG(small_val) AS a"] {
        let (t, n) = agg_col(expr, &schema, &pg_config(), "a");
        assert_eq!(t, SqlType::Decimal, "PG {expr} → numeric (Decimal)");
        assert!(n, "AVG is always nullable");

        let (t, _) = agg_col(expr, &schema, &mysql_config(), "a");
        assert_eq!(t, SqlType::Decimal, "MySQL {expr} → decimal");

        let (t, _) = agg_col(expr, &schema, &sqlite_config(), "a");
        assert_eq!(t, SqlType::Double, "SQLite {expr} → real (Double)");
    }
}

#[test]
fn test_aggregate_avg_decimal_and_double_preserved() {
    let schema = make_numeric_schema();
    for config in [pg_config(), mysql_config(), sqlite_config()] {
        let (t, _) = agg_col("AVG(dec_val) AS a", &schema, &config, "a");
        assert_eq!(t, SqlType::Decimal, "AVG(decimal) must stay Decimal");

        let (t, _) = agg_col("AVG(dbl_val) AS a", &schema, &config, "a");
        assert_eq!(t, SqlType::Double, "AVG(double) must stay Double");
    }
}
