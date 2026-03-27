use super::*;

#[test]
fn test_generate_view_interface() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript }.generate(&schema, &[], &config()).unwrap();
    let src = get_file(&files, "user_summary.ts");
    assert!(src.contains("export interface UserSummary {"));
    assert!(src.contains("id: number;"));
    assert!(src.contains("display_name: string;"));
}
