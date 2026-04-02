use super::*;
use crate::backend::Codegen;
use crate::ir::{Query, ResultColumn};

#[test]
fn test_generate_view_struct() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "user_summary.hpp");
    assert!(src.contains("struct UserSummary"));
    assert!(src.contains("std::int64_t id;"));
    assert!(src.contains("std::string display_name;"));
}

#[test]
fn test_query_can_reuse_view_type() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let query = Query::many(
        "ListUserSummaries",
        "SELECT id, display_name FROM user_summary",
        vec![],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("display_name", SqlType::Text),
        ],
    )
    .with_source_table(Some("user_summary".to_string()));

    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let header = get_file(&files, "queries.hpp");
    assert!(header.contains("#include \"user_summary.hpp\""));
    assert!(header.contains("std::vector<UserSummary> list_user_summaries(pqxx::connection& db);"));
    assert!(!header.contains("struct ListUserSummariesRow"));
}
