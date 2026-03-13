use super::*;
use crate::backend::test_helpers::get_file;
use crate::ir::{Column, Parameter, Query, ResultColumn, Schema, SqlType, Table};

fn schema_with_users() -> Schema {
    Schema {
        tables: vec![Table {
            name: "users".to_string(),
            columns: vec![
                Column { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".to_string(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true, is_primary_key: false },
            ],
        }],
    }
}

fn get_user_query() -> Query {
    Query::one(
        "GetUser",
        "SELECT id, name, email FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true },
        ],
    )
}

fn list_users_query() -> Query {
    Query::many(
        "ListUsers",
        "SELECT id, name, email FROM users",
        vec![],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true },
        ],
    )
}

fn delete_user_query() -> Query {
    Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)])
}

fn delete_users_query() -> Query {
    Query::exec_rows("DeleteUsers", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)])
}

fn config() -> OutputConfig {
    OutputConfig { out: "src".to_string(), package: String::new(), list_params: None }
}

// ─── js_type ─────────────────────────────────────────────────────────────

#[test]
fn test_js_type_primitives() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Boolean, false, &pg), "boolean");
    assert_eq!(js_type(&SqlType::Integer, false, &pg), "number");
    assert_eq!(js_type(&SqlType::BigInt, false, &pg), "number");
    assert_eq!(js_type(&SqlType::Text, false, &pg), "string");
    assert_eq!(js_type(&SqlType::Uuid, false, &pg), "string");
    assert_eq!(js_type(&SqlType::Bytes, false, &pg), "Buffer");
    assert_eq!(js_type(&SqlType::Date, false, &pg), "Date");
    assert_eq!(js_type(&SqlType::Timestamp, false, &pg), "Date");
    assert_eq!(js_type(&SqlType::Json, false, &pg), "unknown");
    // MySQL DATE maps to string (mysql2 returns/expects date strings to avoid timezone issues)
    assert_eq!(js_type(&SqlType::Date, false, &JsTarget::Mysql), "string");
    assert_eq!(js_type(&SqlType::Date, false, &JsTarget::Sqlite), "Date");
}

#[test]
fn test_js_type_nullable() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Text, true, &pg), "string | null");
    assert_eq!(js_type(&SqlType::BigInt, true, &pg), "number | null");
}

#[test]
fn test_js_type_array() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Array(Box::new(SqlType::Integer)), false, &pg), "number[]");
    assert_eq!(js_type(&SqlType::Array(Box::new(SqlType::Text)), true, &pg), "string[] | null");
}

// ─── model file ──────────────────────────────────────────────────────────

#[test]
fn test_ts_model_file_interface() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = gen.emit_model_file(&schema.tables[0]).unwrap();
    assert!(content.contains("export interface Users {"));
    assert!(content.contains("id: number;"));
    assert!(content.contains("name: string;"));
    assert!(content.contains("email: string | null;"));
}

#[test]
fn test_js_model_file_typedef() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript };
    let content = gen.emit_model_file(&schema.tables[0]).unwrap();
    assert!(content.contains("@typedef {Object} Users"));
    assert!(content.contains("@property {number} id"));
    assert!(content.contains("@property {string} name"));
    assert!(content.contains("@property {string | null} email"));
}

// ─── index file ──────────────────────────────────────────────────────────

#[test]
fn test_ts_index_file_with_queries() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = gen.emit_index_file(&schema, &["queries".to_string()]).unwrap();
    assert!(content.contains("export * from './users';"));
    assert!(content.contains("export * from './queries';"));
}

#[test]
fn test_js_index_file_no_queries() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript };
    let content = gen.emit_index_file(&schema, &[]).unwrap();
    assert!(content.contains("export * from './users.js';"));
    assert!(!content.contains("queries"));
}

// ─── pg queries file ─────────────────────────────────────────────────────

#[test]
fn test_pg_ts_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { ClientBase } from 'pg';"));
    assert!(content.contains("import type { Users } from './users';"));
    assert!(content.contains("async function getUser(db: ClientBase, id: number): Promise<Users | null>"));
    assert!(content.contains("db.query<Users>"));
    assert!(content.contains("result.rows[0] ?? null"));
}

#[test]
fn test_pg_ts_many_query() {
    let schema = schema_with_users();
    let queries = vec![list_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("Promise<Users[]>"));
    assert!(content.contains("return result.rows;"));
}

#[test]
fn test_pg_ts_exec_and_execrows() {
    let schema = schema_with_users();
    let queries = vec![delete_user_query(), delete_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("Promise<void>"));
    assert!(content.contains("Promise<number>"));
    assert!(content.contains("result.rowCount ?? 0"));
}

#[test]
fn test_pg_js_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("@typedef {import('pg').ClientBase} ClientBase"));
    assert!(content.contains("@param {ClientBase} db"));
    assert!(content.contains("@param {number} id"));
    assert!(content.contains("@returns {Promise<Users | null>}"));
    assert!(content.contains("export async function getUser(db, id)"));
    assert!(!content.contains("db.query<Users>")); // no generics in JS
    assert!(content.contains("result.rows[0] ?? null"));
}

// ─── sqlite queries file ─────────────────────────────────────────────────

#[test]
fn test_sqlite_ts_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { Database } from 'better-sqlite3';"));
    assert!(content.contains("db: Database"));
    assert!(content.contains(".get(id) as Users | undefined"));
    assert!(content.contains("row ?? null"));
}

#[test]
fn test_sqlite_ts_many_query() {
    let schema = schema_with_users();
    let queries = vec![list_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains(".all() as Users[]"));
}

#[test]
fn test_sqlite_js_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::JavaScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("@typedef {import('better-sqlite3').Database} Database"));
    assert!(content.contains("export async function getUser(db, id)"));
    assert!(!content.contains("as Users")); // no casts in JS
    assert!(content.contains("row ?? null"));
}

// ─── mysql queries file ───────────────────────────────────────────────────

#[test]
fn test_mysql_ts_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Mysql, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { Connection, ResultSetHeader, RowDataPacket } from 'mysql2/promise';"));
    assert!(content.contains("db: Connection"));
    assert!(content.contains("query<RowDataPacket[]>"));
    assert!(content.contains("as Users | undefined"));
    assert!(content.contains("?? null"));
}

#[test]
fn test_mysql_ts_execrows() {
    let schema = schema_with_users();
    let queries = vec![delete_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Mysql, output: JsOutput::TypeScript };
    let content = build_queries_file(&queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("query<ResultSetHeader>"));
    assert!(content.contains("result.affectedRows"));
}

// ─── inline row type ─────────────────────────────────────────────────────

#[test]
fn test_inline_row_type_ts() {
    let query = Query::one(
        "GetStats",
        "SELECT count(*) AS total FROM users",
        vec![],
        vec![ResultColumn { name: "total".to_string(), sql_type: SqlType::BigInt, nullable: true }],
    );
    let mut src = String::new();
    emit_inline_row_type(&mut src, &query, &JsOutput::TypeScript, &JsTarget::Postgres).unwrap();
    assert!(src.contains("export interface GetStatsRow {"));
    assert!(src.contains("total: number | null;"));
}

#[test]
fn test_inline_row_type_js() {
    let query = Query::one(
        "GetStats",
        "SELECT count(*) AS total FROM users",
        vec![],
        vec![ResultColumn { name: "total".to_string(), sql_type: SqlType::BigInt, nullable: true }],
    );
    let mut src = String::new();
    emit_inline_row_type(&mut src, &query, &JsOutput::JavaScript, &JsTarget::Postgres).unwrap();
    assert!(src.contains("@typedef {Object} GetStatsRow"));
    assert!(src.contains("@property {number | null} total"));
}

// ─── list params ─────────────────────────────────────────────────────────

fn list_by_ids_query() -> Query {
    Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    )
}

fn dynamic_cfg() -> OutputConfig {
    OutputConfig { out: "src".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) }
}

#[test]
fn test_pg_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant rewrites IN ($1) → = ANY($1); pg accepts a JS array directly.
    assert!(src_has_sql_constant(&content, "= ANY($1)"), "PG native should rewrite to = ANY");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    assert!(!content.contains("JSON.stringify"), "PG native must not call JSON.stringify");
    // Args array includes ids at its position.
    assert!(content.contains("[ids]"), "ids array passed directly to pg");
}

#[test]
fn test_pg_native_list_param_js() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::JavaScript, &config()).unwrap();
    assert!(src_has_sql_constant(&content, "= ANY($1)"), "PG native should rewrite to = ANY");
    // JS output uses JSDoc comments, not inline TypeScript type annotations.
    assert!(!content.contains("ids: number[]"), "JS output must not use inline TS type annotations");
}

#[test]
fn test_pg_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // Dynamic builds $N numbered placeholders at runtime.
    assert!(content.contains("'$' +"), "PG dynamic builds $N placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args array");
    // Dynamic must NOT emit a SQL constant.
    assert!(!content.contains("GET_BY_IDS"), "dynamic strategy must not emit a SQL constant");
}

#[test]
fn test_pg_dynamic_list_param_with_scalar_before() {
    // Scalar before IN: arg order must be [scalar, ...list].
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("active, ...ids"), "scalar before IN must come first in args");
}

#[test]
fn test_pg_dynamic_list_param_with_scalar_after() {
    // Scalar after IN: arg order must be [...list, scalar].
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("...ids, active"), "scalar after IN must follow list in args");
}

#[test]
fn test_sqlite_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant uses json_each for SQLite.
    assert!(src_has_sql_constant(&content, "json_each"), "SQLite native should use json_each");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // SQLite native uses JSON.stringify at runtime.
    assert!(content.contains("JSON.stringify(ids)"), "SQLite native must JSON.stringify the list");
    assert!(content.contains("idsJson"), "JSON variable should be named idsJson");
    assert!(!content.contains("= ANY"), "SQLite must not use PG ANY syntax");
}

#[test]
fn test_sqlite_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // SQLite dynamic uses anonymous ? placeholders.
    assert!(content.contains(r#"() => "?""#), "SQLite dynamic must use ? placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args");
    assert!(!content.contains("'$' +"), "SQLite dynamic must not use $N placeholders");
}

#[test]
fn test_sqlite_dynamic_list_param_with_scalar_after() {
    // TypeScript correctly places scalar-after after the list (no Bug B for TS).
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("...ids, active"), "scalar after IN must follow list in args");
}

#[test]
fn test_mysql_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant uses JSON_TABLE for MySQL.
    assert!(src_has_sql_constant(&content, "JSON_TABLE"), "MySQL native should use JSON_TABLE");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    assert!(content.contains("JSON.stringify(ids)"), "MySQL native must JSON.stringify the list");
    assert!(content.contains("idsJson"), "JSON variable should be named idsJson");
    assert!(!content.contains("json_each"), "MySQL must not use SQLite json_each");
}

#[test]
fn test_mysql_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // MySQL dynamic uses anonymous ? placeholders (same as SQLite, unlike PG).
    assert!(content.contains(r#"() => "?""#), "MySQL dynamic must use ? placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args");
}

#[test]
fn test_list_param_text_type_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("tags: string[]"), "Text list param should use string[] type");
}

#[test]
fn test_list_param_execrows_pg_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteByIds", "DELETE FROM t WHERE id IN ($1)", vec![Parameter::list(1, "ids", SqlType::BigInt, false)]);
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("Promise<number>"), "execrows should return Promise<number>");
    assert!(content.contains("rowCount"), "execrows should use rowCount");
}

// ─── generate: nullable params ───────────────────────────────────────────

#[test]
fn test_nullable_param_pg_ts() {
    // Nullable param → `T | null` in function signature; pg passes null directly.
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
    assert!(!content.contains("id: number | null"), "non-nullable param must not be nullable");
}

#[test]
fn test_nullable_param_sqlite_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
}

#[test]
fn test_nullable_param_mysql_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
}

/// Check whether the generated content's SQL constants block contains `needle`.
fn src_has_sql_constant(content: &str, needle: &str) -> bool {
    // SQL constants are emitted as: CONST_NAME = "...sql..."
    // They appear before the function definitions.
    content.contains(needle)
}

// ─── generate: query grouping ────────────────────────────────────────────

fn pg_ts() -> TypeScriptCodegen {
    TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript }
}

fn ts_cfg() -> crate::config::OutputConfig {
    crate::config::OutputConfig { out: "out".to_string(), package: String::new(), list_params: None }
}

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"users.ts"), "{names:?}");
    assert!(names.contains(&"posts.ts"), "{names:?}");
    assert!(names.contains(&"index.ts"), "{names:?}");
    assert!(!names.contains(&"queries.ts"), "queries.ts must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let users_src = get_file(&files, "users.ts");
    let posts_src = get_file(&files, "posts.ts");
    assert!(users_src.contains("getUser"), "users.ts must contain getUser");
    assert!(!users_src.contains("getPost"), "users.ts must not contain getPost");
    assert!(posts_src.contains("getPost"), "posts.ts must contain getPost");
    assert!(!posts_src.contains("getUser"), "posts.ts must not contain getUser");
}

#[test]
fn test_generate_grouped_index_exports_all_groups() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let index_src = get_file(&files, "index.ts");
    assert!(index_src.contains("from './users'"), "index.ts must re-export users");
    assert!(index_src.contains("from './posts'"), "index.ts must re-export posts");
}
