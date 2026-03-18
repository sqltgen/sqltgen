use super::*;

#[test]
fn cte_basic_resolves_columns() {
    let sql = "-- name: GetRecentPosts :many\n\
        WITH recent AS (SELECT id, title FROM posts)\n\
        SELECT id, title FROM recent;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "title"]);
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn cte_param_in_outer_where() {
    // $1 is in the outer WHERE, bound to a column from the CTE
    let sql = "-- name: GetUserPosts :many\n\
        WITH uposts AS (SELECT id, user_id, title FROM posts)\n\
        SELECT id, title FROM uposts WHERE user_id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "user_id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn cte_chained() {
    // Second CTE references the first CTE
    let sql = "-- name: GetTitles :many\n\
        WITH base AS (SELECT id, title FROM posts),\n\
             titled AS (SELECT title FROM base)\n\
        SELECT title FROM titled;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["title"]);
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
}

#[test]
fn cte_joined_with_schema_table() {
    // CTE is JOINed with a real schema table
    let sql = "-- name: GetUserPostTitles :many\n\
        WITH uposts AS (SELECT user_id, title FROM posts)\n\
        SELECT u.name, p.title FROM users u JOIN uposts p ON p.user_id = u.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["name", "title"]);
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn cte_update_body_params_are_typed_from_schema() {
    // WITH up AS (UPDATE … SET qty=$1 WHERE sku=$2) INSERT …
    // $1 and $2 should be typed from the UPDATE CTE body, not fallback Text.
    let sql = "-- name: UpsertStock :one\n\
        WITH up AS ( \
            UPDATE inventory SET qty = $1 WHERE sku = $2 RETURNING sku, qty \
        ) \
        INSERT INTO inventory (sku, qty) SELECT $2, $1 \
        WHERE NOT EXISTS (SELECT 1 FROM up) \
        RETURNING sku, qty;";
    let q = &parse_queries(sql, &make_inventory_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    // $1 = qty, $2 = sku (first-appearance order from named-param rewrite / schema)
    let qty_param = q.params.iter().find(|p| p.index == 1).unwrap();
    let sku_param = q.params.iter().find(|p| p.index == 2).unwrap();
    assert_eq!(qty_param.sql_type, SqlType::Integer, "$1 should be qty (Integer)");
    assert_eq!(sku_param.sql_type, SqlType::Text, "$2 should be sku (Text)");
}

#[test]
fn cte_update_body_result_columns_from_insert_returning() {
    // RETURNING on the outer INSERT should produce typed result columns.
    let sql = "-- name: UpsertStock :one\n\
        WITH up AS ( \
            UPDATE inventory SET qty = $1 WHERE sku = $2 RETURNING sku, qty \
        ) \
        INSERT INTO inventory (sku, qty) SELECT $2, $1 \
        WHERE NOT EXISTS (SELECT 1 FROM up) \
        RETURNING sku, qty;";
    let q = &parse_queries(sql, &make_inventory_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "sku");
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert_eq!(q.result_columns[1].name, "qty");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Integer);
}

#[test]
fn cte_insert_returning_columns_flow_to_outer_select() {
    // WITH inserted AS (INSERT … RETURNING …) SELECT * FROM inserted
    // The outer SELECT * should expand to the RETURNING columns.
    let sql = "-- name: CreateUser :one\n\
        WITH ins AS (\
            INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email\
        )\
        SELECT * FROM ins;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 3, "should have id, name, email from RETURNING");
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email"]);
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn execrows_cte_with_params_keeps_method_params_when_type_inference_fails() {
    let sql = "-- name: ArchiveOldSessions :execrows\n\
        with moved as (\n\
          delete from sessions\n\
          where created_at < @cutoff\n\
            and (@tenant_id = -1 or tenant_id = @tenant_id)\n\
          returning id, tenant_id\n\
        )\n\
        update tenants\n\
        set active_sessions = active_sessions - 1\n\
        from moved\n\
        where tenants.id = moved.tenant_id;";

    let schema = Schema {
        tables: vec![
            Table::new(
                "sessions".into(),
                vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "tenant_id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                    Column { name: "created_at".into(), sql_type: SqlType::Timestamp, nullable: false, is_primary_key: false },
                ],
            ),
            Table::new(
                "tenants".into(),
                vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "active_sessions".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                ],
            ),
        ],
        ..Default::default()
    };

    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.cmd, QueryCmd::ExecRows);
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "cutoff");
    assert_eq!(q.params[0].sql_type, SqlType::Timestamp, "cutoff should be typed from sessions.created_at");
    assert_eq!(q.params[1].name, "tenant_id");
    assert_eq!(q.params[1].sql_type, SqlType::BigInt, "tenant_id should be typed from sessions.tenant_id");
    assert_eq!(q.sql.matches("$1").count(), 1);
    assert_eq!(q.sql.matches("$2").count(), 2);
}
