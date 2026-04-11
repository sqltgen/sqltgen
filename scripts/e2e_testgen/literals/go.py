"""Go literal renderers for abstract typed values.

Implements the extended generator protocol for Go's static type system:
  - render_typed_arg: uses manifest lang_type for type-correct arg construction
  - render_call_lines: wraps calls with Go error-handling pattern
  - transform_field_expr: converts snake_case field names to PascalCase
  - render_assert_eq_typed: type-aware assertions (NullTime, JSON, etc.)
  - render_assert_null_typed / render_assert_not_null_typed: nil vs Valid checks
"""

from __future__ import annotations

import re
from typing import Any


# ── Protocol: optional extension methods ────────────────────────────────

def step_indent() -> str:
    """Go uses tabs for indentation."""
    return "\t"


def assign_op() -> str:
    """Go uses := for short variable declarations."""
    return ":="


def func_prefix() -> str:
    """Generated query functions live in the 'gen' package."""
    return "gen."


def conn_param() -> str:
    """Go generated functions take (ctx, db) as first two parameters."""
    return "ctx, db"


def null_literal() -> str:
    return "nil"


# ── render_value: for let-step values and assertion expected values ──────

def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract value as a Go expression (type inferred by context)."""
    if kind == "str":
        return _go_str(value)
    elif kind == "int":
        return str(int(value))
    elif kind == "float":
        return str(float(value))
    elif kind == "bool":
        return "true" if value else "false"
    elif kind == "null":
        return "nil"
    elif kind == "json":
        # Return Go map literal — used for assertions and let-steps.
        # Call args use render_typed_arg which wraps in the right Go type.
        return _to_go_map(value)
    elif kind == "uuid":
        return "genUUID()" if value == "random" else _go_str(value)
    elif kind == "datetime":
        return _parse_go_datetime(str(value))
    elif kind == "date":
        return _parse_go_date(str(value))
    elif kind == "time":
        return _parse_go_time(str(value))
    elif kind == "list":
        if not value:
            return "nil"
        parts = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            parts.append(render_value(str(item_kind), item_val, engine, coercions))
        return f"{{{', '.join(parts)}}}"
    elif kind == "var":
        return str(value)
    else:
        raise ValueError(f"Unknown value type: {kind}")


# ── render_typed_arg: uses manifest lang_type for type-correct construction ──

def render_typed_arg(
    arg_name: str,
    lang_type: str,
    kind: str,
    value: Any,
    engine: str,
    coercions: dict[str, str],
) -> str:
    """Render a call argument using its exact Go lang_type from the manifest."""
    if kind == "null":
        return _go_null(lang_type)
    if kind == "list":
        import re
        m = re.match(r"\[]\*?(.+)", lang_type)
        elem_lang_type = m.group(1) if m else ""
        if not value:
            go_type = lang_type
            if _is_enum_type(elem_lang_type):
                go_type = f"[]gen.{elem_lang_type}"
            return f"{go_type}{{}}"
        elements = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            elements.append(
                render_typed_arg("_", elem_lang_type, str(item_kind), item_val, engine, coercions)
            )
        # Prefix enum slice types with gen. so they resolve in test files.
        go_type = lang_type
        if _is_enum_type(elem_lang_type):
            go_type = f"[]gen.{elem_lang_type}"
        return f"{go_type}{{{', '.join(elements)}}}"
    if kind == "json":
        return _go_json_arg(lang_type, value)
    if kind == "datetime":
        dt = _parse_go_datetime(str(value))
        if lang_type == "sql.NullTime":
            return f"sql.NullTime{{Time: {dt}, Valid: true}}"
        return dt
    if kind == "date":
        d = _parse_go_date(str(value))
        if lang_type == "sql.NullTime":
            return f"sql.NullTime{{Time: {d}, Valid: true}}"
        return d
    if kind == "time":
        # Go database/sql drivers (lib/pq, go-sqlite3, go-sql-driver/mysql) return
        # TIME columns as strings, which sql.Scan cannot convert to time.Time.
        # Always store null to avoid scan failures on read-back.
        return "sql.NullTime{}"
    if kind == "var":
        var_name = str(value)
        if lang_type == "sql.NullTime":
            return f"sql.NullTime{{Time: {var_name}, Valid: true}}"
        return var_name
    if kind == "str":
        # Pointer-to-enum: *Priority → allocate and return pointer
        if lang_type.startswith("*") and _is_enum_type(lang_type[1:]):
            inner = lang_type[1:]
            escaped = _escape_go_str(str(value))
            return f'_genPtrTo(gen.{inner}("{escaped}"))'
        # Non-pointer enum: Priority → type conversion
        if _is_enum_type(lang_type):
            return f'gen.{lang_type}("{_escape_go_str(str(value))}")'
        # sql.NullString: wrap in Valid struct
        if lang_type == "sql.NullString":
            return f'sql.NullString{{String: {_go_str(value)}, Valid: true}}'

    if kind == "int":
        n = str(int(value))
        if lang_type == "sql.NullInt32":
            return f"sql.NullInt32{{Int32: {n}, Valid: true}}"
        if lang_type == "sql.NullInt64":
            return f"sql.NullInt64{{Int64: {n}, Valid: true}}"
        return n

    if kind == "float":
        # Go maps NUMERIC/DECIMAL to string via database/sql.
        if lang_type == "string":
            return f'"{value}"'
        if lang_type == "sql.NullString":
            return f'sql.NullString{{String: "{value}", Valid: true}}'
        return str(float(value))

    if kind == "bool":
        return "true" if value else "false"

    if kind == "uuid":
        if str(value) == "random":
            return "genUUID()"
        return _go_str(str(value))

    # Fallback for unknown kinds.
    return render_value(kind, value, engine, coercions)


# ── render_call_lines: Go-style error handling ───────────────────────────

def render_call_lines(
    call_expr: str,
    bind: str | None,
    command: str,
    indent: str,
    null_checked_vars: set[str] | None = None,
) -> list[str]:
    """Render a function call as Go source lines with error handling."""
    if bind:
        lines = [
            f"{indent}{bind}, err := {call_expr}",
            f"{indent}if err != nil {{",
            f"{indent}\tt.Fatal(err)",
            f"{indent}}}",
        ]
        # Suppress "declared and not used" for variables that may only be
        # read in later steps (Go vet checks per-function, not per-scope).
        if command in ("one", "many"):
            lines.append(f"{indent}_ = {bind}")
        return lines
    # No bind: discard return value if command returns a result (one/many).
    if command in ("one", "many"):
        return [
            f"{indent}if _, err := {call_expr}; err != nil {{",
            f"{indent}\tt.Fatal(err)",
            f"{indent}}}",
        ]
    # exec/execrows without bind: use inline if err := ...
    return [
        f"{indent}if err := {call_expr}; err != nil {{",
        f"{indent}\tt.Fatal(err)",
        f"{indent}}}",
    ]


# ── Field expression transformation ──────────────────────────────────────

def transform_field_expr(expr: str) -> str:
    """Convert snake_case struct field names to PascalCase for Go.

    Transforms the field-access segments of an expression:
    'ev.event_date' → 'ev.EventDate', 'events[0].name' → 'events[0].Name'.
    The receiver variable and array indexing are left unchanged.
    """
    return re.sub(r"\.([a-z][a-z0-9_]*)", lambda m: "." + _to_pascal(m.group(1)), expr)


# ── Assertion renderers ───────────────────────────────────────────────────

def render_assert_eq(field_expr: str, expected: str) -> str:
    """Fallback equality assertion (used when render_assert_eq_typed is not called)."""
    return f"if {field_expr} != {expected} {{ t.Errorf(\"expected %v, got %v\", {expected}, {field_expr}) }}"


def render_assert_eq_typed(
    field_expr: str,
    expected: str,
    kind: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Type-aware equality assertion.

    For datetime/date/time: uses _genTimeOf to extract time.Time from sql.NullTime.
    For json: uses genAssertJSON which handles []byte and string uniformly.
    For other types: direct != comparison.
    """
    if kind == "time":
        # Go drivers return TIME as a string; sql.Scan cannot convert it to time.Time.
        return f"// {field_expr}: TIME scan unsupported in Go drivers; assertion skipped"
    if kind in ("datetime", "date", "time"):
        # expected is a time.Date(...) expression (from render_value).
        # _genTimeOf extracts time.Time from both time.Time and sql.NullTime.
        return (
            f"if !_genTimeOf({field_expr}).Equal({expected}) {{"
            f" t.Errorf(\"expected %v, got %v\", {expected}, _genTimeOf({field_expr})) }}"
        )
    if kind == "json":
        # expected is a map[string]interface{}{...} expression.
        # genAssertJSON handles []byte, *[]byte, sql.NullString, and string payloads.
        return f"genAssertJSON(t, {field_expr}, {expected})"
    if kind == "str" and field_lang_type and _is_enum_type(field_lang_type):
        enum_expected = f'gen.{field_lang_type}({expected})'
        return f"if {field_expr} != {enum_expected} {{ t.Errorf(\"expected %v, got %v\", {enum_expected}, {field_expr}) }}"
    if kind == "str" and field_lang_type == "sql.NullString":
        return (
            f"if !{field_expr}.Valid || {field_expr}.String != {expected} "
            f"{{ t.Errorf(\"expected %v, got %v\", {expected}, {field_expr}) }}"
        )
    if kind == "int" and field_lang_type == "sql.NullInt32":
        return (
            f"if !{field_expr}.Valid || {field_expr}.Int32 != {expected} "
            f"{{ t.Errorf(\"expected %v, got %v\", {expected}, {field_expr}) }}"
        )
    if kind == "int" and field_lang_type == "sql.NullInt64":
        return (
            f"if !{field_expr}.Valid || {field_expr}.Int64 != {expected} "
            f"{{ t.Errorf(\"expected %v, got %v\", {expected}, {field_expr}) }}"
        )
    return f"if {field_expr} != {expected} {{ t.Errorf(\"expected %v, got %v\", {expected}, {field_expr}) }}"


def render_assert_json_eq(field_expr: str, value: Any, field_lang_type: str | None = None) -> str:
    """JSON equality assertion for json_string coercion (SQLite).

    Both field and expected value go through genAssertJSON which parses
    both sides before comparing, tolerating key-ordering differences.
    """
    go_map = _to_go_map(value)
    return f"genAssertJSON(t, {field_expr}, {go_map})"


def render_assert_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Null assertion.

    Direct variables (result pointers) use != nil.
    Struct fields with nullable types use _genIsNull which handles
    sql.NullTime, sql.NullString, and pointer types uniformly.
    """
    if "." not in expr:
        # expr is a bare variable (e.g. result *Event pointer)
        return f"if {expr} != nil {{ t.Fatalf(\"expected nil, got %v\", {expr}) }}"
    return f"if !_genIsNull({expr}) {{ t.Fatalf(\"expected null {expr}\") }}"


def render_assert_not_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Not-null assertion.

    Direct variables (result pointers) use == nil.
    Struct fields use _genIsNull for uniformity.
    """
    if "." not in expr:
        return f"if {expr} == nil {{ t.Fatalf(\"expected non-nil {expr}\") }}"
    return f"if _genIsNull({expr}) {{ t.Fatalf(\"expected non-null {expr}\") }}"


def render_assert_null(expr: str) -> str:
    """Fallback null assertion (no engine context)."""
    return f"if {expr} != nil {{ t.Fatalf(\"expected nil, got %v\", {expr}) }}"


def render_assert_not_null(expr: str) -> str:
    """Fallback not-null assertion."""
    return f"if {expr} == nil {{ t.Fatalf(\"expected non-nil {expr}\") }}"


def render_assert_len(var_expr: str, length: str) -> str:
    """Length assertion."""
    return f"if len({var_expr}) != {length} {{ t.Fatalf(\"expected len={length}, got %d\", len({var_expr})) }}"


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """UUID string comparison."""
    return f"if {field_expr} != {var_name} {{ t.Errorf(\"expected doc_id=%s, got %s\", {var_name}, {field_expr}) }}"


# ── Internal helpers ─────────────────────────────────────────────────────


_GO_BUILTIN_TYPES = frozenset({
    "string", "int", "int8", "int16", "int32", "int64",
    "uint", "uint8", "uint16", "uint32", "uint64",
    "float32", "float64", "bool", "byte", "rune",
    "any", "interface{}",
})

_GO_STDLIB_PREFIXES = ("sql.", "time.", "[]", "*")


def _is_enum_type(lang_type: str) -> bool:
    """Return True if lang_type looks like a generated Go enum type.

    Go enums are PascalCase identifiers (e.g. Priority, Status).
    Builtins (string, int64, etc.) and stdlib types (sql.NullString, time.Time)
    are not enums.
    """
    if lang_type in _GO_BUILTIN_TYPES:
        return False
    if any(lang_type.startswith(p) for p in _GO_STDLIB_PREFIXES):
        return False
    # Must be PascalCase: starts with uppercase letter, no dots
    return bool(lang_type) and lang_type[0].isupper() and "." not in lang_type


def _escape_go_str(s: str) -> str:
    """Escape a string for use inside a Go double-quoted string literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _go_str(value: Any) -> str:
    """Render a string as a Go quoted string literal."""
    # repr() produces valid Go string literals for simple strings.
    s = str(value)
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _go_null(lang_type: str) -> str:
    """Return the Go zero/null value for a given lang_type."""
    if lang_type == "sql.NullTime":
        return "sql.NullTime{}"
    if lang_type == "sql.NullString":
        return "sql.NullString{}"
    if lang_type == "sql.NullInt64":
        return "sql.NullInt64{}"
    if lang_type.startswith("*"):
        return "nil"
    return "nil"


def _go_json_arg(lang_type: str, value: Any) -> str:
    """Render a JSON value as the correct Go type based on lang_type."""
    go_map = _to_go_map(value)
    if lang_type == "[]byte":
        return f"mustJSON({go_map})"
    if lang_type == "*[]byte":
        return f"ptrBytes(mustJSON({go_map}))"
    if lang_type == "sql.NullString":
        return f"sql.NullString{{String: string(mustJSON({go_map})), Valid: true}}"
    # string (SQLite/MySQL non-nullable JSON stored as TEXT)
    return f"string(mustJSON({go_map}))"


def _parse_go_datetime(s: str) -> str:
    """Parse a datetime string and return a Go time.Date(...) expression."""
    s = s.rstrip("Z").replace("T", " ")
    parts = s.split(" ")
    date_parts = parts[0].split("-")
    time_parts = parts[1].split(":") if len(parts) > 1 else ["0", "0", "0"]
    year, month, day = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
    hour = int(time_parts[0])
    minute = int(time_parts[1])
    sec = int(time_parts[2]) if len(time_parts) > 2 else 0
    return f"time.Date({year}, {month}, {day}, {hour}, {minute}, {sec}, 0, time.UTC)"


def _parse_go_date(s: str) -> str:
    """Parse a date string (YYYY-MM-DD) and return a Go time.Date(...) expression."""
    parts = str(s).split("-")
    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    return f"time.Date({year}, {month}, {day}, 0, 0, 0, 0, time.UTC)"


def _parse_go_time(s: str) -> str:
    """Parse a time string (HH:MM:SS) and return a Go time.Date(...) expression.

    Uses year 1 as the base date since Go's zero-time handling for year 0 can
    be inconsistent across SQLite and MySQL drivers.
    """
    parts = str(s).split(":")
    hour = int(parts[0])
    minute = int(parts[1])
    sec = int(parts[2]) if len(parts) > 2 else 0
    return f"time.Date(1, 1, 1, {hour}, {minute}, {sec}, 0, time.UTC)"


def _to_go_map(value: Any) -> str:
    """Convert a Python value to a Go map/slice/literal expression."""
    if isinstance(value, dict):
        if not value:
            return "map[string]interface{}{}"
        items = ", ".join(f'"{k}": {_to_go_map(v)}' for k, v in value.items())
        return "map[string]interface{}{" + items + "}"
    if isinstance(value, list):
        if not value:
            return "[]interface{}{}"
        return "[]interface{}{" + ", ".join(_to_go_map(v) for v in value) + "}"
    if isinstance(value, str):
        return _go_str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "nil"
    return str(value)


def _to_pascal(s: str) -> str:
    """Convert a snake_case identifier to PascalCase."""
    return "".join(w.capitalize() for w in s.split("_"))
