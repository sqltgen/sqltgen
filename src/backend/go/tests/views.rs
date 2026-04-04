use super::*;

#[test]
fn test_generate_view_struct() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "models.go");
    assert!(src.contains("type UserSummary struct {"));
    assert!(src.contains("Id\tint64"));
    assert!(src.contains("DisplayName\tstring"));
}
