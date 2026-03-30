use super::*;

fn make_nest_schema() -> Schema {
    Schema {
        tables: vec![
            Table::new(
                "users",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("name", SqlType::Text),
                    Column::new_not_nullable("email", SqlType::Text),
                ],
            ),
            Table::new(
                "companies",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("name", SqlType::Text),
                    Column::new_not_nullable("sector", SqlType::Text),
                ],
            ),
            Table::new(
                "user_companies",
                vec![
                    Column::new_not_nullable("user_id", SqlType::BigInt),
                    Column::new_not_nullable("company_id", SqlType::BigInt),
                ],
            ),
            Table::new(
                "addresses",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("user_id", SqlType::BigInt),
                    Column::new_not_nullable("street", SqlType::Text),
                    Column::new_not_nullable("city", SqlType::Text),
                ],
            ),
        ],
        ..Default::default()
    }
}

// ─── Annotation parsing ──────────────────────────────────────────────────────

#[test]
fn nest_annotation_basic() {
    let sql = "\
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name, company_sector)
SELECT u.id, u.name, u.email,
       c.id AS company_id, c.name AS company_name, c.sector AS company_sector
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert_eq!(q.nested_groups.len(), 1);
    assert_eq!(q.nested_groups[0].field_name, "company");
    assert_eq!(q.nested_groups[0].columns.len(), 3);
}

#[test]
fn nest_annotation_auto_prefix_strip() {
    let sql = "\
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name, company_sector)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name, c.sector AS company_sector
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    let targets: Vec<&str> = group.columns.iter().map(|c| c.target_name.as_str()).collect();
    assert_eq!(targets, ["id", "name", "sector"]);
}

#[test]
fn nest_annotation_explicit_alias() {
    let sql = "\
-- name: GetUserCompanies :many
-- nest: companies(c_id as id, c_name as name)
SELECT u.id, u.name,
       c.id AS c_id, c.name AS c_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    assert_eq!(group.field_name, "companies");
    let targets: Vec<&str> = group.columns.iter().map(|c| c.target_name.as_str()).collect();
    assert_eq!(targets, ["id", "name"]);
}

#[test]
fn nest_annotation_explicit_alias_accepts_uppercase_as() {
    let sql = "\
-- name: GetUserCompanies :many
-- nest: companies(c_id AS id, c_name AS name)
SELECT u.id, u.name,
       c.id AS c_id, c.name AS c_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    assert_eq!(group.field_name, "companies");
    let targets: Vec<&str> = group.columns.iter().map(|c| c.target_name.as_str()).collect();
    assert_eq!(targets, ["id", "name"]);
}

#[test]
fn nest_annotation_preserves_types() {
    let sql = "\
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    assert_eq!(group.columns[0].sql_type, SqlType::BigInt);
    assert_eq!(group.columns[1].sql_type, SqlType::Text);
}

#[test]
fn nest_parent_columns_exclude_nested() {
    let sql = "\
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let parent_names: Vec<&str> = q.parent_columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(parent_names, ["id", "name"]);
}

#[test]
fn nest_with_left_join_makes_nested_nullable() {
    let sql = "\
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
LEFT JOIN user_companies uc ON uc.user_id = u.id
LEFT JOIN companies c ON c.id = uc.company_id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    assert!(group.columns[0].nullable, "company_id should be nullable from LEFT JOIN");
    assert!(group.columns[1].nullable, "company_name should be nullable from LEFT JOIN");
}

#[test]
fn nest_multiple_groups() {
    let sql = "\
-- name: GetUserFull :many
-- nest: company(company_id, company_name)
-- nest: address(address_id, address_street, address_city)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name,
       a.id AS address_id, a.street AS address_street, a.city AS address_city
FROM users u
LEFT JOIN user_companies uc ON uc.user_id = u.id
LEFT JOIN companies c ON c.id = uc.company_id
LEFT JOIN addresses a ON a.user_id = u.id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert_eq!(q.nested_groups.len(), 2);
    assert_eq!(q.nested_groups[0].field_name, "company");
    assert_eq!(q.nested_groups[1].field_name, "address");
    let parent_names: Vec<&str> = q.parent_columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(parent_names, ["id", "name"]);
}

#[test]
fn nest_no_prefix_keeps_original_name() {
    let sql = "\
-- name: GetUserWithAddr :many
-- nest: addr(street, city)
SELECT u.id, u.name,
       a.street, a.city
FROM users u
JOIN addresses a ON a.user_id = u.id";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    let group = &q.nested_groups[0];
    let targets: Vec<&str> = group.columns.iter().map(|c| c.target_name.as_str()).collect();
    assert_eq!(targets, ["street", "city"], "columns without the field prefix keep their original name");
}

#[test]
fn nest_query_without_annotation_has_no_groups() {
    let sql = "\
-- name: GetUser :one
SELECT id, name, email FROM users WHERE id = $1";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert!(q.nested_groups.is_empty());
    assert!(!q.has_nested_groups());
}

#[test]
fn nest_ignores_unknown_columns() {
    let sql = "\
-- name: GetUser :many
-- nest: x(nonexistent_col)
SELECT id, name FROM users";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert!(q.nested_groups.is_empty(), "group with no matching columns should be dropped");
}

#[test]
fn nest_one_query_command() {
    let sql = "\
-- name: GetUserWithCompanies :one
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
LEFT JOIN user_companies uc ON uc.user_id = u.id
LEFT JOIN companies c ON c.id = uc.company_id
WHERE u.id = $1";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert!(q.has_nested_groups());
    assert_eq!(q.cmd, QueryCmd::One);
    assert_eq!(q.nested_groups[0].field_name, "company");
}

#[test]
fn nest_rejected_for_exec_queries() {
    let sql = "\
-- name: DeleteUser :exec
-- nest: company(company_id, company_name)
DELETE FROM users WHERE id = $1";
    let queries = parse_queries(sql, &make_nest_schema()).expect("parser should still succeed globally");
    assert!(
        queries.is_empty(),
        "-- nest: on :exec must reject this query (current parser policy: warn + drop invalid query)"
    );
}

#[test]
fn nest_rejected_for_execrows_queries() {
    let sql = "\
-- name: DeleteUsers :execrows
-- nest: company(company_id, company_name)
DELETE FROM users WHERE id = $1";
    let queries = parse_queries(sql, &make_nest_schema()).expect("parser should still succeed globally");
    assert!(
        queries.is_empty(),
        "-- nest: on :execrows must reject this query (current parser policy: warn + drop invalid query)"
    );
}

#[test]
fn nest_rejected_with_list_params() {
    let sql = "\
-- name: GetUsersWithCompanies :many
-- @ids bigint[] not null
-- nest: company(company_id, company_name)
SELECT u.id, u.name, c.id AS company_id, c.name AS company_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id
WHERE u.id IN (@ids)";
    let queries = parse_queries(sql, &make_nest_schema()).expect("parser should still succeed globally");
    assert!(
        queries.is_empty(),
        "-- nest: with list params must reject this query until explicitly supported"
    );
}

#[test]
fn nest_annotation_stripped_from_stored_sql() {
    let sql = "\
-- name: Q :many
-- nest: company(company_id)
SELECT u.id, c.id AS company_id
FROM users u
JOIN companies c ON 1=1";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert!(!q.sql.contains("nest:"), "stored SQL should not contain nest annotations");
}

#[test]
fn nest_rejected_with_invalid_js_field_name() {
    let sql = "\
-- name: Q :many
-- nest: my-field(company_id)
SELECT u.id, c.id AS company_id
FROM users u
JOIN companies c ON 1=1";
    let queries = parse_queries(sql, &make_nest_schema()).expect("parser should still succeed globally");
    assert!(
        queries.is_empty(),
        "invalid nest field name should reject the query (warn + drop policy)"
    );
}

#[test]
fn nest_allows_same_source_column_in_multiple_groups() {
    let sql = "\
-- name: Q :many
-- nest: company(company_id)
-- nest: partner(company_id)
SELECT u.id, c.id AS company_id
FROM users u
JOIN companies c ON 1=1";
    let q = &parse_queries(sql, &make_nest_schema()).unwrap()[0];
    assert_eq!(q.nested_groups.len(), 2);
    assert_eq!(q.nested_groups[0].field_name, "company");
    assert_eq!(q.nested_groups[1].field_name, "partner");
    let parent_names: Vec<&str> = q.parent_columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(parent_names, ["id"]);
}
