use super::*;

#[test]
fn test_generate_view_record() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "UserSummary.java");
    assert!(src.contains("public record UserSummary("));
    assert!(src.contains("long id"));
    assert!(src.contains("String displayName"));
}
