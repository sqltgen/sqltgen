use crate::ir::QueryCmd;

pub(in crate::frontend::common) struct QueryAnnotation {
    pub name: String,
    pub cmd: QueryCmd,
}

pub(super) fn split_into_blocks(sql: &str) -> Vec<(QueryAnnotation, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<QueryAnnotation> = None;
    let mut body_lines: Vec<&str> = Vec::new();

    for line in sql.lines() {
        if let Some(ann) = parse_annotation(line) {
            flush_block(&mut current, &mut body_lines, &mut blocks);
            current = Some(ann);
        } else if current.is_some() {
            body_lines.push(line);
        }
    }
    flush_block(&mut current, &mut body_lines, &mut blocks);
    blocks
}

fn flush_block(current: &mut Option<QueryAnnotation>, lines: &mut Vec<&str>, out: &mut Vec<(QueryAnnotation, String)>) {
    if let Some(ann) = current.take() {
        let body = lines.join("\n");
        let body = body.trim().to_string();
        if !body.is_empty() {
            out.push((ann, body));
        }
    }
    lines.clear();
}

fn parse_annotation(line: &str) -> Option<QueryAnnotation> {
    let line = line.trim();
    // -- name: Foo :one
    let rest = line.strip_prefix("--")?.trim();
    let rest = rest.strip_prefix("name:")?.trim();
    let mut parts = rest.splitn(2, ':');
    let name = parts.next()?.trim().to_string();
    let cmd_str = parts.next()?.trim().to_lowercase();
    let cmd = match cmd_str.as_str() {
        "one" => QueryCmd::One,
        "many" => QueryCmd::Many,
        "exec" => QueryCmd::Exec,
        "execrows" => QueryCmd::ExecRows,
        _ => return None,
    };
    if name.is_empty() {
        return None;
    }
    Some(QueryAnnotation { name, cmd })
}
