use super::*;

#[test]
fn union_all_produces_typed_result_columns() {
    let schema = make_schema();
    let sql = "-- name: UnionAll :many\n\
        SELECT id, name FROM users WHERE id = $1\n\
        UNION ALL\n\
        SELECT id, name FROM users WHERE id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    // Result columns come from the left branch
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    // Params from both branches
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
}

#[test]
fn union_distinct_produces_typed_result_columns() {
    let schema = make_schema();
    let sql = "-- name: UnionDistinct :many\n\
        SELECT id, name FROM users WHERE name = $1\n\
        UNION\n\
        SELECT id, name FROM users WHERE name = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].sql_type, SqlType::Text);
    assert_eq!(q.params[1].sql_type, SqlType::Text);
}

#[test]
fn intersect_produces_typed_result_columns() {
    let schema = make_schema();
    let sql = "-- name: Intersect :many\n\
        SELECT id, name FROM users WHERE id = $1\n\
        INTERSECT\n\
        SELECT id, name FROM users WHERE id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.params.len(), 2);
}

#[test]
fn except_produces_typed_result_columns() {
    let schema = make_schema();
    let sql = "-- name: Except :many\n\
        SELECT id, name FROM users WHERE id = $1\n\
        EXCEPT\n\
        SELECT id, name FROM users WHERE id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.params.len(), 2);
}

#[test]
fn triple_union_all_collects_all_params() {
    // Three branches chained: UNION ALL of UNION ALL
    let schema = make_schema();
    let sql = "-- name: TripleUnion :many\n\
        SELECT id, name FROM users WHERE id = $1\n\
        UNION ALL\n\
        SELECT id, name FROM users WHERE id = $2\n\
        UNION ALL\n\
        SELECT id, name FROM users WHERE id = $3;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    assert_eq!(q.params[2].sql_type, SqlType::BigInt);
}

#[test]
fn union_all_with_join_infers_params() {
    let schema = make_join_schema();
    let sql = "-- name: UnionJoin :many\n\
        SELECT u.id, u.name FROM users u JOIN posts p ON p.user_id = u.id WHERE p.id = $1\n\
        UNION ALL\n\
        SELECT u.id, u.name FROM users u WHERE u.id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    // Second param also resolves to "id" column, gets dedup suffix
    assert_eq!(q.params[1].name, "id_2");
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
}

#[test]
fn union_all_no_params_still_typed() {
    let schema = make_schema();
    let sql = "-- name: UnionNoParams :many\n\
        SELECT id, name FROM users\n\
        UNION ALL\n\
        SELECT id, name FROM users;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params.len(), 0);
}
