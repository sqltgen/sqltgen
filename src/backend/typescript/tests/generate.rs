use super::*;
use crate::backend::test_helpers::cfg;

// ─── model file ──────────────────────────────────────────────────────────

#[test]
fn test_ts_model_file_interface() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = gen.emit_model_file(&schema.tables[0], &cfg()).unwrap();
    assert!(content.contains("export interface Users {"));
    assert!(content.contains("id: number;"));
    assert!(content.contains("name: string;"));
    assert!(content.contains("email: string | null;"));
}

#[test]
fn test_js_model_file_typedef() {
    let schema = schema_with_users();
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript };
    let content = gen.emit_model_file(&schema.tables[0], &cfg()).unwrap();
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
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { ConnectFn, Db } from './_sqltgen';"));
    assert!(content.contains("import { releaseDb } from './_sqltgen';"));
    assert!(content.contains("import type { Users } from './users';"));
    assert!(content.contains("async function getUser(db: Db, id: number): Promise<Users | null>"));
    assert!(content.contains("db.query<Users>"));
    assert!(content.contains("result.rows[0] ?? null"));
}

#[test]
fn test_pg_ts_many_query() {
    let schema = schema_with_users();
    let queries = vec![list_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("Promise<Users[]>"));
    assert!(content.contains("return result.rows;"));
}

#[test]
fn test_pg_ts_exec_and_execrows() {
    let schema = schema_with_users();
    let queries = vec![delete_user_query(), delete_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("Promise<void>"));
    assert!(content.contains("Promise<number>"));
    assert!(content.contains("result.rowCount ?? 0"));
}

#[test]
fn test_pg_js_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("@typedef {import('./_sqltgen.js').Db} Db"));
    assert!(content.contains("@typedef {import('./_sqltgen.js').ConnectFn} ConnectFn"));
    assert!(content.contains("@param {Db} db"));
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
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { ConnectFn, Db } from './_sqltgen';"));
    assert!(content.contains("db: Db"));
    assert!(content.contains(".get(id) as Users | undefined"));
    assert!(content.contains("row ?? null"));
}

#[test]
fn test_sqlite_ts_many_query() {
    let schema = schema_with_users();
    let queries = vec![list_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::TypeScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains(".all() as Users[]"));
}

#[test]
fn test_sqlite_js_one_query() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::JavaScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("@typedef {import('./_sqltgen.js').Db} Db"));
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
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("import type { ConnectFn, Db } from './_sqltgen';"));
    assert!(content.contains("db: Db"));
    assert!(content.contains("query<RowDataPacket[]>"));
    assert!(content.contains("as Users | undefined"));
    assert!(content.contains("?? null"));
}

#[test]
fn test_mysql_ts_execrows() {
    let schema = schema_with_users();
    let queries = vec![delete_users_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Mysql, output: JsOutput::TypeScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("query<ResultSetHeader>"));
    assert!(content.contains("result.affectedRows"));
}

#[test]
fn test_ts_querier_wrapper_is_emitted() {
    let schema = schema_with_users();
    let queries = vec![get_user_query()];
    let gen = TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript };
    let content = build_queries_file("", &queries, &schema, &gen.target, &gen.output, &config()).unwrap();
    assert!(content.contains("export class Querier {"));
    assert!(content.contains("constructor(private readonly connect: ConnectFn)"));
    assert!(content.contains("async getUser(id: number): Promise<Users | null>"));
    assert!(content.contains("const db = await this.connect();"));
    assert!(content.contains("return getUser(db, id);"));
    assert!(content.contains("await releaseDb(db);"));
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
    emit_inline_row_type(&mut src, &query, &JsOutput::TypeScript, &JsTarget::Postgres, &cfg()).unwrap();
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
    emit_inline_row_type(&mut src, &query, &JsOutput::JavaScript, &JsTarget::Postgres, &cfg()).unwrap();
    assert!(src.contains("@typedef {Object} GetStatsRow"));
    assert!(src.contains("@property {number | null} total"));
}
