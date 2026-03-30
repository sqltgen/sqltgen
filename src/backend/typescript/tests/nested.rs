use super::*;
use crate::frontend::postgres::query as pg_frontend_query;
use crate::ir::{NestedColumn, NestedGroup};

fn nested_many_query() -> Query {
    let mut q = Query::many(
        "GetUserWithCompanies",
        "SELECT u.id, u.name, c.id AS company_id, c.name AS company_name, c.sector AS company_sector FROM users u JOIN companies c ON c.user_id = u.id",
        vec![],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::not_nullable("company_id", SqlType::BigInt),
            ResultColumn::not_nullable("company_name", SqlType::Text),
            ResultColumn::not_nullable("company_sector", SqlType::Text),
        ],
    );
    q.nested_groups = vec![NestedGroup {
        field_name: "company".to_string(),
        columns: vec![
            NestedColumn { source_name: "company_id".to_string(), target_name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            NestedColumn { source_name: "company_name".to_string(), target_name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            NestedColumn { source_name: "company_sector".to_string(), target_name: "sector".to_string(), sql_type: SqlType::Text, nullable: false },
        ],
    }];
    q
}

fn nested_one_query() -> Query {
    let mut q = Query::one(
        "GetUserWithCompanies",
        "SELECT u.id, u.name, c.id AS company_id, c.name AS company_name FROM users u LEFT JOIN companies c ON c.user_id = u.id WHERE u.id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("company_id", SqlType::BigInt),
            ResultColumn::nullable("company_name", SqlType::Text),
        ],
    );
    q.nested_groups = vec![NestedGroup {
        field_name: "company".to_string(),
        columns: vec![
            NestedColumn { source_name: "company_id".to_string(), target_name: "id".to_string(), sql_type: SqlType::BigInt, nullable: true },
            NestedColumn { source_name: "company_name".to_string(), target_name: "name".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    }];
    q
}

fn nested_multi_group_query() -> Query {
    let mut q = Query::many(
        "GetUserFull",
        "SELECT u.id, u.name, c.id AS company_id, c.name AS company_name, a.id AS addr_id, a.city AS addr_city FROM users u LEFT JOIN companies c ON c.user_id = u.id LEFT JOIN addresses a ON a.user_id = u.id",
        vec![],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("company_id", SqlType::BigInt),
            ResultColumn::nullable("company_name", SqlType::Text),
            ResultColumn::nullable("addr_id", SqlType::BigInt),
            ResultColumn::nullable("addr_city", SqlType::Text),
        ],
    );
    q.nested_groups = vec![
        NestedGroup {
            field_name: "company".to_string(),
            columns: vec![
                NestedColumn { source_name: "company_id".to_string(), target_name: "id".to_string(), sql_type: SqlType::BigInt, nullable: true },
                NestedColumn { source_name: "company_name".to_string(), target_name: "name".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        },
        NestedGroup {
            field_name: "addr".to_string(),
            columns: vec![
                NestedColumn { source_name: "addr_id".to_string(), target_name: "id".to_string(), sql_type: SqlType::BigInt, nullable: true },
                NestedColumn { source_name: "addr_city".to_string(), target_name: "city".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        },
    ];
    q
}

fn roundtrip_nest_schema() -> Schema {
    Schema {
        tables: vec![
            Table::new(
                "users",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("name", SqlType::Text),
                    Column::new_not_nullable("status", SqlType::Text),
                    Column::new_not_nullable("region", SqlType::Text),
                ],
            ),
            Table::new(
                "companies",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("name", SqlType::Text),
                ],
            ),
            Table::new(
                "user_companies",
                vec![
                    Column::new_not_nullable("user_id", SqlType::BigInt),
                    Column::new_not_nullable("company_id", SqlType::BigInt),
                ],
            ),
        ],
        ..Default::default()
    }
}

fn nested_many_query_with_two_params() -> Query {
    let mut q = Query::many(
        "GetUserWithCompaniesByFilter",
        "SELECT u.id, u.name, c.id AS company_id, c.name AS company_name FROM users u JOIN user_companies uc ON uc.user_id = u.id JOIN companies c ON c.id = uc.company_id WHERE u.status = $1 AND u.region = $2",
        vec![
            Parameter::scalar(1, "status", SqlType::Text, false),
            Parameter::scalar(2, "region", SqlType::Text, false),
        ],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::not_nullable("company_id", SqlType::BigInt),
            ResultColumn::not_nullable("company_name", SqlType::Text),
        ],
    );
    q.nested_groups = vec![NestedGroup {
        field_name: "company".to_string(),
        columns: vec![
            NestedColumn { source_name: "company_id".to_string(), target_name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            NestedColumn { source_name: "company_name".to_string(), target_name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
        ],
    }];
    q
}

// ─── Type generation ─────────────────────────────────────────────────────────

#[test]
fn pg_ts_nested_many_emits_flat_interface() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("interface _GetUserWithCompaniesFlatRow {"), "should emit private flat row interface");
    assert!(content.contains("company_id: number;"), "flat row should have company_id");
    assert!(content.contains("company_name: string;"), "flat row should have company_name");
    assert!(content.contains("company_sector: string;"), "flat row should have company_sector");
}

#[test]
fn pg_ts_nested_many_emits_child_interface() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("export interface GetUserWithCompanies_Company {"), "should emit child type");
    assert!(content.contains("  id: number;"), "child type should have mapped 'id' field");
    assert!(content.contains("  name: string;"), "child type should have mapped 'name' field");
    assert!(content.contains("  sector: string;"), "child type should have mapped 'sector' field");
}

#[test]
fn pg_ts_nested_many_emits_parent_interface() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("export interface GetUserWithCompaniesRow {"), "should emit parent row type");
    assert!(content.contains("  company: GetUserWithCompanies_Company[];"), "parent should have nested array field");
}

#[test]
fn pg_ts_nested_many_parent_excludes_nested_columns() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    let parent_block = extract_block(&content, "export interface GetUserWithCompaniesRow {");
    assert!(!parent_block.contains("company_id"), "parent should not have company_id as direct field");
    assert!(!parent_block.contains("company_name"), "parent should not have company_name as direct field");
}

#[test]
fn pg_ts_nested_many_aggregation_logic() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("const grouped = new Map<string, GetUserWithCompaniesRow>()"), "should create Map for grouping");
    assert!(content.contains("const seen_company = new Set<string>()"), "should create dedup Set per group");
    assert!(content.contains("JSON.stringify([row.id, row.name])"), "should build key from parent columns");
    assert!(content.contains("if (row.company_id != null)"), "should null-check before pushing nested");
    assert!(content.contains("!seen_company.has(company_key)"), "should deduplicate children");
    assert!(content.contains("parent.company.push("), "should push to nested array");
    assert!(content.contains("Array.from(grouped.values())"), "should return aggregated values");
}

#[test]
fn pg_ts_nested_many_return_type() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("Promise<GetUserWithCompaniesRow[]>"), "return type should be nested row array");
}

#[test]
fn pg_ts_nested_one_aggregation_logic() {
    let schema = Schema::default();
    let queries = vec![nested_one_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("Promise<GetUserWithCompaniesRow | null>"), ":one should return single row or null");
    assert!(content.contains("if (result.rows.length === 0) return null;"), "should return null for empty result");
    assert!(content.contains("const parent: GetUserWithCompaniesRow"), "should create single parent");
    assert!(content.contains("result.rows[0].id"), ":one parent should init from first row, not undefined 'row'");
    assert!(content.contains("const seen_company = new Set<string>()"), "should create dedup Set");
    assert!(content.contains("!seen_company.has(company_key)"), "should deduplicate children");
    assert!(content.contains("parent.company.push("), "should push nested items into parent");
    assert!(content.contains("return parent;"), "should return the single parent");
}

#[test]
fn pg_ts_nested_multi_group() {
    let schema = Schema::default();
    let queries = vec![nested_multi_group_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("export interface GetUserFull_Company {"), "should emit first child type");
    assert!(content.contains("export interface GetUserFull_Addr {"), "should emit second child type");
    assert!(content.contains("  company: GetUserFull_Company[];"), "parent should have company array");
    assert!(content.contains("  addr: GetUserFull_Addr[];"), "parent should have addr array");
}

// ─── SQLite target ───────────────────────────────────────────────────────────

#[test]
fn sqlite_ts_nested_many_aggregation() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("as _GetUserWithCompaniesFlatRow[]"), "should cast raw rows to flat type");
    assert!(content.contains("const grouped = new Map<string, GetUserWithCompaniesRow>()"), "should create grouping map");
    assert!(content.contains("const seen_company = new Set<string>()"), "should create dedup Set");
    assert!(content.contains("Array.from(grouped.values())"), "should return aggregated values");
}

#[test]
fn sqlite_ts_nested_one() {
    let schema = Schema::default();
    let queries = vec![nested_one_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("if (rows.length === 0) return null;"), "should return null for empty result");
    assert!(content.contains("const parent: GetUserWithCompaniesRow"), "should create single parent");
    assert!(content.contains("rows[0].id"), ":one parent should init from first row");
}

// ─── MySQL target ────────────────────────────────────────────────────────────

#[test]
fn mysql_ts_nested_many_aggregation() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("as unknown as _GetUserWithCompaniesFlatRow[]"), "should cast mysql rows to flat type");
    assert!(content.contains("const grouped = new Map<string, GetUserWithCompaniesRow>()"), "should create grouping map");
    assert!(content.contains("const seen_company = new Set<string>()"), "should create dedup Set");
}

#[test]
fn mysql_ts_nested_one() {
    let schema = Schema::default();
    let queries = vec![nested_one_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("if (typed.length === 0) return null;"), "should return null for empty result");
    assert!(content.contains("typed[0].id"), ":one parent should init from first row");
    assert!(content.contains("return parent;"), "should return the single parent");
}

// ─── JavaScript output ───────────────────────────────────────────────────────

#[test]
fn pg_js_nested_many_emits_jsdoc_types() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::JavaScript, &config()).unwrap();
    assert!(content.contains("@typedef {Object} GetUserWithCompanies_Company"), "should emit child JSDoc typedef");
    assert!(content.contains("@typedef {Object} GetUserWithCompaniesRow"), "should emit parent JSDoc typedef");
    assert!(content.contains("@property {GetUserWithCompanies_Company[]} company"), "parent typedef should have nested array");
}

#[test]
fn pg_js_nested_many_no_generics() {
    let schema = Schema::default();
    let queries = vec![nested_many_query()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::JavaScript, &config()).unwrap();
    assert!(!content.contains("Map<string"), "JS output should not have Map generics");
    assert!(!content.contains("Set<string"), "JS output should not have Set generics");
    assert!(!content.contains("interface "), "JS output should not have TypeScript interfaces");
}

#[test]
fn pg_ts_nested_many_with_two_params_emits_args_array() {
    let schema = Schema::default();
    let queries = vec![nested_many_query_with_two_params()];
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(
        content.contains("SQL_GET_USER_WITH_COMPANIES_BY_FILTER, [status, region]"),
        "nested query should pass both params in order to db.query"
    );
}

#[test]
fn pg_ts_nested_with_list_param_is_rejected() {
    let mut q = nested_many_query();
    q.params = vec![Parameter::list(1, "ids", SqlType::BigInt, false)];
    let schema = Schema::default();
    let err = build_queries_file("", &[q], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).expect_err("nested + list should fail");
    assert!(
        err.to_string().contains("nested results with list params"),
        "unexpected error: {err}"
    );
}

#[test]
fn nested_roundtrip_parse_to_codegen_smoke() {
    let schema = roundtrip_nest_schema();
    let sql = "\
-- name: GetUsersWithCompaniesByFilter :many
-- @status text not null
-- @region text not null
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id
WHERE u.status = @status AND u.region = @region";
    let queries = pg_frontend_query::parse_queries(sql, &schema).expect("frontend parse should work");
    let content = build_queries_file("", &queries, &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("export interface GetUsersWithCompaniesByFilter_Company {"), "round-trip should emit nested child type");
    assert!(
        content.contains("SQL_GET_USERS_WITH_COMPANIES_BY_FILTER, [status, region]"),
        "round-trip should preserve params in generated args"
    );
    assert!(!content.contains("nest:"), "generated output should not contain nest annotation comments");
}

// ─── Helper ──────────────────────────────────────────────────────────────────

fn extract_block(content: &str, start_marker: &str) -> String {
    let start = content.find(start_marker).unwrap_or(0);
    let rest = &content[start..];
    let end = rest.find('}').map(|i| i + 1).unwrap_or(rest.len());
    rest[..end].to_string()
}
