use super::*;

#[test]
fn test_generate_view_data_class() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "UserSummary.kt");
    assert!(src.contains("data class UserSummary("));
    assert!(src.contains("val id: Long"));
    assert!(src.contains("val displayName: String"));
}
