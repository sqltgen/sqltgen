pub(crate) mod named_params;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod typemap;

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, DataType, Expr, Ident, ObjectName, ObjectNamePart, RenameTableNameKind, TableConstraint,
};

use crate::ir::{Column, SqlType, Table};

// ─── Dialect DDL configuration ───────────────────────────────────────────────

/// Bundles the dialect-specific knobs needed for DDL processing.
///
/// Every DDL helper function needs the same two pieces of dialect knowledge:
/// how to map `sqlparser` `DataType` nodes to `SqlType`, and which
/// `ALTER TABLE` operations the dialect supports. Grouping them avoids
/// threading two separate arguments through every call in the chain.
#[derive(Clone, Copy)]
pub(crate) struct DdlDialect {
    /// Maps a sqlparser `DataType` to the canonical `SqlType` for this dialect.
    pub map_type: fn(&DataType) -> SqlType,
    /// Which `ALTER TABLE` operations this dialect supports.
    pub alter_caps: AlterCaps,
}

/// Controls which `ALTER TABLE` operations the common handler will apply.
///
/// Different SQL dialects support different subsets. SQLite, for instance,
/// only supports `ADD COLUMN`, `RENAME COLUMN`, and `RENAME TABLE`.  Passing
/// the correct caps ensures the handler won't silently apply unsupported ops.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AlterCaps {
    pub add_column: bool,
    pub drop_column: bool,
    pub alter_column: bool,
    pub rename_column: bool,
    pub rename_table: bool,
    pub add_constraint: bool,
}

impl AlterCaps {
    /// All operations supported (PostgreSQL, MySQL).
    pub const ALL: Self = Self { add_column: true, drop_column: true, alter_column: true, rename_column: true, rename_table: true, add_constraint: true };

    /// Only the operations SQLite supports: ADD COLUMN, RENAME COLUMN, RENAME TABLE.
    pub const SQLITE: Self = Self { add_column: true, drop_column: false, alter_column: false, rename_column: true, rename_table: true, add_constraint: false };
}

/// Removes tables named in a `DROP TABLE` statement.
pub(crate) fn apply_drop_tables(names: &[ObjectName], tables: &mut Vec<Table>) {
    for name in names {
        let table_name = obj_name_to_str(name);
        tables.retain(|t| t.name != table_name);
    }
}

/// Applies `ALTER TABLE` operations to the in-memory table list.
///
/// Only operations enabled in `dialect.alter_caps` are applied; the rest are
/// silently skipped. This prevents the handler from applying operations that
/// the target dialect does not actually support.
pub(crate) fn apply_alter_table(name: &ObjectName, operations: &[AlterTableOperation], tables: &mut [Table], dialect: DdlDialect) {
    let table_name = obj_name_to_str(name);
    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return;
    };

    let caps = dialect.alter_caps;
    for op in operations {
        let table = &mut tables[idx];
        match op {
            AlterTableOperation::AddColumn { column_def, .. } if caps.add_column => {
                table.columns.push(build_column(column_def, dialect.map_type));
            },
            AlterTableOperation::DropColumn { column_names, .. } if caps.drop_column => {
                let names: Vec<String> = column_names.iter().map(ident_to_str).collect();
                table.columns.retain(|c| !names.contains(&c.name));
            },
            AlterTableOperation::AlterColumn { column_name, op } if caps.alter_column => {
                let col_name = ident_to_str(column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == col_name) {
                    match op {
                        AlterColumnOperation::SetNotNull => col.nullable = false,
                        AlterColumnOperation::DropNotNull => col.nullable = true,
                        AlterColumnOperation::SetDataType { data_type, .. } => {
                            col.sql_type = (dialect.map_type)(data_type);
                        },
                        _ => {},
                    }
                }
            },
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } if caps.rename_column => {
                let old = ident_to_str(old_column_name);
                let new = ident_to_str(new_column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == old) {
                    col.name = new;
                }
            },
            AlterTableOperation::RenameTable { table_name: new_name } if caps.rename_table => {
                let obj_name = match new_name {
                    RenameTableNameKind::As(n) | RenameTableNameKind::To(n) => n,
                };
                table.name = obj_name_to_str(obj_name);
            },
            AlterTableOperation::AddConstraint { constraint, .. } if caps.add_constraint => {
                let pk_cols = pk_columns_from_constraint(constraint);
                for col in table.columns.iter_mut() {
                    if pk_cols.contains(&col.name) {
                        col.is_primary_key = true;
                        col.nullable = false;
                    }
                }
            },
            _ => {},
        }
    }
}

// ─── Identifier helpers ───────────────────────────────────────────────────────

/// Converts an identifier to a string, preserving case for quoted identifiers
/// and lowercasing bare ones.
pub(crate) fn ident_to_str(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Returns the last component of a dotted name (e.g. `schema.table` → `table`).
pub(crate) fn obj_name_to_str(name: &ObjectName) -> String {
    name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None }).unwrap_or_default()
}

/// Extracts PRIMARY KEY column names from a table-level constraint, if any.
pub(crate) fn pk_columns_from_constraint(tc: &TableConstraint) -> Vec<String> {
    match tc {
        TableConstraint::PrimaryKey(pk) => {
            pk.columns.iter().filter_map(|ic| if let Expr::Identifier(ident) = &ic.column.expr { Some(ident_to_str(ident)) } else { None }).collect()
        },
        _ => vec![],
    }
}

// ─── Column / table builders ─────────────────────────────────────────────────

/// Builds a [`Column`] from an AST column definition.
///
/// `map_type` is the dialect-specific type mapper (e.g. `postgres::typemap::map`
/// or `sqlite::typemap::map`).
pub(crate) fn build_column(col_def: &ColumnDef, map_type: fn(&DataType) -> SqlType) -> Column {
    let name = ident_to_str(&col_def.name);
    let sql_type = map_type(&col_def.data_type);

    let mut nullable = true;
    let mut is_primary_key = false;

    for opt_def in &col_def.options {
        match &opt_def.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::PrimaryKey(_) => {
                is_primary_key = true;
                nullable = false;
            },
            // GENERATED ALWAYS AS IDENTITY implies non-null (PostgreSQL; harmless on SQLite)
            ColumnOption::Generated { .. } => nullable = false,
            _ => {},
        }
    }

    Column { name, sql_type, nullable, is_primary_key }
}

/// Builds a [`Table`] from a `CREATE TABLE` AST node.
pub(crate) fn build_create_table(name: &ObjectName, column_defs: &[ColumnDef], constraints: &[TableConstraint], dialect: DdlDialect) -> Table {
    let table_name = obj_name_to_str(name);

    // Collect table-level PRIMARY KEY column names
    let mut pk_cols: Vec<String> = Vec::new();
    for constraint in constraints {
        pk_cols.extend(pk_columns_from_constraint(constraint));
    }

    let mut columns: Vec<Column> = column_defs.iter().map(|col_def| build_column(col_def, dialect.map_type)).collect();

    // Promote columns that appear in a table-level PRIMARY KEY
    for col in &mut columns {
        if pk_cols.contains(&col.name) {
            col.is_primary_key = true;
            col.nullable = false;
        }
    }

    Table::new(table_name, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    /// Trivial type mapper for tests — maps everything the same way a real dialect would
    /// for the basic types we use in test DDL.
    fn test_map(dt: &DataType) -> SqlType {
        match dt {
            DataType::Integer(_) | DataType::Int(_) => SqlType::Integer,
            DataType::BigInt(_) => SqlType::BigInt,
            DataType::Text => SqlType::Text,
            DataType::Boolean | DataType::Bool => SqlType::Boolean,
            DataType::Varchar(_) => SqlType::VarChar(None),
            DataType::Numeric(_) | DataType::Decimal(_) => SqlType::Decimal,
            _ => SqlType::Custom(format!("{dt}").to_lowercase()),
        }
    }

    fn test_dialect() -> DdlDialect {
        DdlDialect { map_type: test_map, alter_caps: AlterCaps::ALL }
    }

    /// Helper: parse DDL with the generic dialect and return the first CREATE TABLE.
    fn parse_create_table(ddl: &str) -> Table {
        let stmts = Parser::parse_sql(&GenericDialect {}, ddl).unwrap();
        for stmt in stmts {
            if let sqlparser::ast::Statement::CreateTable(ct) = stmt {
                return build_create_table(&ct.name, &ct.columns, &ct.constraints, test_dialect());
            }
        }
        panic!("no CREATE TABLE found");
    }

    // ─── ident_to_str ────────────────────────────────────────────────────────

    #[test]
    fn test_ident_to_str_bare_is_lowercased() {
        let ident = Ident::new("FooBar");
        assert_eq!(ident_to_str(&ident), "foobar");
    }

    #[test]
    fn test_ident_to_str_quoted_preserves_case() {
        let ident = Ident::with_quote('"', "FooBar");
        assert_eq!(ident_to_str(&ident), "FooBar");
    }

    // ─── obj_name_to_str ─────────────────────────────────────────────────────

    #[test]
    fn test_obj_name_to_str_simple_name() {
        let name = ObjectName::from(vec![Ident::new("users")]);
        assert_eq!(obj_name_to_str(&name), "users");
    }

    #[test]
    fn test_obj_name_to_str_dotted_returns_last() {
        let name = ObjectName::from(vec![Ident::new("public"), Ident::new("Users")]);
        assert_eq!(obj_name_to_str(&name), "users");
    }

    #[test]
    fn test_obj_name_to_str_empty_returns_empty() {
        let name = ObjectName(vec![]);
        assert_eq!(obj_name_to_str(&name), "");
    }

    // ─── build_column ────────────────────────────────────────────────────────

    #[test]
    fn test_build_column_nullable_by_default() {
        let t = parse_create_table("CREATE TABLE t (bio TEXT);");
        assert!(t.columns[0].nullable);
        assert!(!t.columns[0].is_primary_key);
        assert_eq!(t.columns[0].sql_type, SqlType::Text);
    }

    #[test]
    fn test_build_column_not_null() {
        let t = parse_create_table("CREATE TABLE t (name TEXT NOT NULL);");
        assert!(!t.columns[0].nullable);
    }

    #[test]
    fn test_build_column_inline_primary_key() {
        let t = parse_create_table("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[0].nullable);
        assert!(!t.columns[1].is_primary_key);
    }

    #[test]
    fn test_build_column_explicit_null_overrides_not_null() {
        // Pathological but valid: NOT NULL then NULL — last one wins
        let t = parse_create_table("CREATE TABLE t (x TEXT NOT NULL NULL);");
        assert!(t.columns[0].nullable);
    }

    // ─── build_create_table ──────────────────────────────────────────────────

    #[test]
    fn test_build_create_table_table_level_pk() {
        let t = parse_create_table("CREATE TABLE kv (k TEXT NOT NULL, v TEXT NOT NULL, PRIMARY KEY (k));");
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[0].nullable);
        assert!(!t.columns[1].is_primary_key);
    }

    #[test]
    fn test_build_create_table_composite_pk() {
        let t = parse_create_table("CREATE TABLE edges (src BIGINT NOT NULL, dst BIGINT NOT NULL, PRIMARY KEY (src, dst));");
        assert!(t.columns[0].is_primary_key);
        assert!(t.columns[1].is_primary_key);
    }

    #[test]
    fn test_build_create_table_name_lowercased() {
        let t = parse_create_table("CREATE TABLE MyTable (id INTEGER PRIMARY KEY);");
        assert_eq!(t.name, "mytable");
    }

    // ─── pk_columns_from_constraint ──────────────────────────────────────────

    #[test]
    fn test_pk_columns_from_non_pk_constraint_returns_empty() {
        // A UNIQUE constraint should return empty
        let stmts = Parser::parse_sql(&GenericDialect {}, "CREATE TABLE t (a TEXT, b TEXT, UNIQUE (a));").unwrap();
        if let sqlparser::ast::Statement::CreateTable(ct) = &stmts[0] {
            for c in &ct.constraints {
                let cols = pk_columns_from_constraint(c);
                assert!(cols.is_empty());
            }
        }
    }

    // ─── apply_drop_tables ───────────────────────────────────────────────────

    #[test]
    fn test_apply_drop_tables_removes_matching() {
        let mut tables = vec![Table::new("a", vec![]), Table::new("b", vec![]), Table::new("c", vec![])];
        let names = vec![ObjectName::from(vec![Ident::new("a")]), ObjectName::from(vec![Ident::new("c")])];
        apply_drop_tables(&names, &mut tables);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "b");
    }

    #[test]
    fn test_apply_drop_tables_ignores_nonexistent() {
        let mut tables = vec![Table::new("a", vec![])];
        let names = vec![ObjectName::from(vec![Ident::new("ghost")])];
        apply_drop_tables(&names, &mut tables);
        assert_eq!(tables.len(), 1);
    }

    // ─── apply_alter_table (integration) ─────────────────────────────────────

    #[test]
    fn test_apply_alter_table_unknown_table_is_noop() {
        let mut tables = vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer)])];
        let stmts = Parser::parse_sql(&GenericDialect {}, "ALTER TABLE ghost ADD COLUMN x TEXT;").unwrap();
        if let sqlparser::ast::Statement::AlterTable(a) = &stmts[0] {
            apply_alter_table(&a.name, &a.operations, &mut tables, DdlDialect { map_type: test_map, alter_caps: AlterCaps::ALL });
        }
        assert_eq!(tables[0].columns.len(), 1);
    }

    #[test]
    fn test_apply_alter_table_sqlite_caps_ignore_drop_column() {
        let mut tables = vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer), Column::new("bio", SqlType::Text)])];
        let stmts = Parser::parse_sql(&GenericDialect {}, "ALTER TABLE users DROP COLUMN bio;").unwrap();
        if let sqlparser::ast::Statement::AlterTable(a) = &stmts[0] {
            apply_alter_table(&a.name, &a.operations, &mut tables, DdlDialect { map_type: test_map, alter_caps: AlterCaps::SQLITE });
        }
        // DROP COLUMN should be ignored under SQLite caps
        assert_eq!(tables[0].columns.len(), 2);
    }

    #[test]
    fn test_apply_alter_table_all_caps_apply_drop_column() {
        let mut tables = vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer), Column::new("bio", SqlType::Text)])];
        let stmts = Parser::parse_sql(&GenericDialect {}, "ALTER TABLE users DROP COLUMN bio;").unwrap();
        if let sqlparser::ast::Statement::AlterTable(a) = &stmts[0] {
            apply_alter_table(&a.name, &a.operations, &mut tables, DdlDialect { map_type: test_map, alter_caps: AlterCaps::ALL });
        }
        // DROP COLUMN should work under ALL caps
        assert_eq!(tables[0].columns.len(), 1);
    }

    #[test]
    fn test_apply_alter_table_sqlite_caps_allow_add_and_rename() {
        let mut tables = vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer)])];
        // ADD COLUMN — should work
        let stmts = Parser::parse_sql(&GenericDialect {}, "ALTER TABLE users ADD COLUMN name TEXT NOT NULL;").unwrap();
        if let sqlparser::ast::Statement::AlterTable(a) = &stmts[0] {
            apply_alter_table(&a.name, &a.operations, &mut tables, DdlDialect { map_type: test_map, alter_caps: AlterCaps::SQLITE });
        }
        assert_eq!(tables[0].columns.len(), 2);
        assert_eq!(tables[0].columns[1].name, "name");

        // RENAME COLUMN — should work
        let stmts = Parser::parse_sql(&GenericDialect {}, "ALTER TABLE users RENAME COLUMN name TO full_name;").unwrap();
        if let sqlparser::ast::Statement::AlterTable(a) = &stmts[0] {
            apply_alter_table(&a.name, &a.operations, &mut tables, DdlDialect { map_type: test_map, alter_caps: AlterCaps::SQLITE });
        }
        assert_eq!(tables[0].columns[1].name, "full_name");
    }
}
