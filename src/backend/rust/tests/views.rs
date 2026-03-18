use super::*;

#[test]
fn test_generate_view_struct() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "user_summary.rs");
    assert!(src.contains("pub struct UserSummary"));
    assert!(src.contains("pub id: i64,"));
    assert!(src.contains("pub display_name: String,"));
}
