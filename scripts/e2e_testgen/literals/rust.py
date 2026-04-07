"""Rust literal renderers for abstract typed values.

Implements the extended generator protocol for Rust's static type system:
  - render_typed_arg: uses manifest lang_type for type-correct arg construction
  - render_call_lines: wraps async calls with .await.unwrap() and let bindings
  - render_assert_eq_typed: type-aware assertions using field_lang_type
  - render_assert_null_typed / render_assert_not_null_typed: Option checks
  - stmt_terminator: returns ";" so let-step lines get semicolons
"""

from __future__ import annotations

import json
from typing import Any


# ── Protocol: optional extension methods ────────────────────────────────

def step_indent() -> str:
    """Rust uses 4-space indentation."""
    return "    "


def assign_op() -> str:
    """Rust uses = for assignment (decl_prefix supplies 'let ')."""
    return "="


def decl_prefix() -> str:
    """Rust requires 'let ' for variable declarations."""
    return "let "


def func_prefix() -> str:
    """Generated query functions live in the 'queries' module."""
    return "queries::"


def conn_param() -> str:
    """Rust generated functions take &pool as the first parameter."""
    return "&pool"


def stmt_terminator() -> str:
    """Rust statements end with a semicolon."""
    return ";"


def null_literal() -> str:
    return "None"


# ── render_value: for let-step values and assertion expected values ──────

def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract value as a Rust expression (raw type, not Option-wrapped)."""
    if kind == "str":
        return f'"{_escape_str(str(value))}".to_string()'
    elif kind == "int":
        return str(int(value))
    elif kind == "float":
        return str(float(value))
    elif kind == "bool":
        return "true" if value else "false"
    elif kind == "null":
        return "None"
    elif kind == "json":
        return _rust_json_value(value, coercions)
    elif kind == "uuid":
        return _rust_uuid_value(value, coercions)
    elif kind == "datetime":
        return _rust_primitive_datetime(str(value))
    elif kind == "date":
        return _rust_date(str(value))
    elif kind == "time":
        return _rust_time(str(value))
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
    """Render a call argument using its exact Rust lang_type from the manifest."""
    is_option = lang_type.startswith("Option<")
    inner_type = lang_type[7:-1] if is_option else lang_type

    if kind == "null":
        return "None"

    if kind == "json":
        json_expr = _rust_json_for_type(inner_type, value)
        return f"Some({json_expr})" if is_option else json_expr

    if kind == "datetime":
        dt_expr = _rust_datetime_for_type(inner_type, str(value))
        return f"Some({dt_expr})" if is_option else dt_expr

    if kind == "date":
        d_expr = _rust_date(str(value))
        return f"Some({d_expr})" if is_option else d_expr

    if kind == "time":
        t_expr = _rust_time(str(value))
        return f"Some({t_expr})" if is_option else t_expr

    if kind == "uuid":
        u_expr = _rust_uuid_for_type(inner_type, value)
        return f"Some({u_expr})" if is_option else u_expr

    if kind == "str":
        if _is_enum_type(inner_type):
            e_expr = f'"{_escape_str(str(value))}".parse::<{inner_type}>().unwrap()'
            return f"Some({e_expr})" if is_option else e_expr
        s_expr = f'"{_escape_str(str(value))}".to_string()'
        return f"Some({s_expr})" if is_option else s_expr

    if kind == "int":
        n = str(int(value))
        return f"Some({n})" if is_option else n

    if kind == "var":
        var_name = str(value)
        need_clone = "String" in inner_type
        clone_suffix = ".clone()" if need_clone else ""
        return f"Some({var_name}{clone_suffix})" if is_option else f"{var_name}{clone_suffix}"

    # Fallback to render_value for unknown kinds
    return render_value(kind, value, engine, coercions)


# ── render_call_lines: Rust async call with .await.unwrap() ──────────────

def render_call_lines(
    call_expr: str,
    bind: str | None,
    command: str,
    indent: str,
    null_checked_vars: set[str] | None = None,
) -> list[str]:
    """Render an async function call as Rust source lines."""
    if bind:
        return [f"{indent}let {bind} = {call_expr}.await.unwrap();"]
    if command in ("one", "many"):
        return [f"{indent}let _ = {call_expr}.await.unwrap();"]
    return [f"{indent}{call_expr}.await.unwrap();"]


# ── Assertion renderers ───────────────────────────────────────────────────

def render_assert_eq(field_expr: str, expected: str) -> str:
    """Fallback equality assertion."""
    return f"assert_eq!({field_expr}, {expected});"


def render_assert_eq_typed(
    field_expr: str,
    expected: str,
    kind: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Type-aware equality assertion using field_lang_type when available."""
    is_option = field_lang_type is not None and field_lang_type.startswith("Option<")
    inner_type = field_lang_type[7:-1] if is_option and field_lang_type is not None else (field_lang_type or "")

    if kind == "json":
        # For SQLite (json_string coercion) codegen.py routes to render_assert_json_eq instead,
        # so here expected is always a serde_json::Value expression (PG/MySQL).
        if is_option:
            return f"assert_eq!({field_expr}, Some({expected}));"
        return f"assert_eq!({field_expr}, {expected});"

    if kind == "datetime":
        # Upgrade to OffsetDateTime (UTC) if the field requires it.
        dt_expr = expected
        if "OffsetDateTime" in inner_type and dt_expr.startswith("datetime!(") and " UTC" not in dt_expr:
            dt_expr = dt_expr[:-1] + " UTC)"
        wrapped = f"Some({dt_expr})" if is_option else dt_expr
        return f"assert_eq!({field_expr}, {wrapped});"

    if kind in ("date", "time"):
        wrapped = f"Some({expected})" if is_option else expected
        return f"assert_eq!({field_expr}, {wrapped});"

    if kind == "int":
        wrapped = f"Some({expected})" if is_option else expected
        return f"assert_eq!({field_expr}, {wrapped});"

    if kind == "str":
        if field_lang_type and _is_enum_type(inner_type):
            # expected is '"value".to_string()'; replace suffix to parse as enum
            enum_expected = expected.replace(".to_string()", f".parse::<{inner_type}>().unwrap()")
            wrapped = f"Some({enum_expected})" if is_option else enum_expected
            return f"assert_eq!({field_expr}, {wrapped});"
        wrapped = f"Some({expected})" if is_option else expected
        return f"assert_eq!({field_expr}, {wrapped});"

    wrapped = f"Some({expected})" if is_option else expected
    return f"assert_eq!({field_expr}, {wrapped});"


def render_assert_json_eq(field_expr: str, value: Any, field_lang_type: str | None = None) -> str:
    """JSON equality assertion for json_string coercion (SQLite).

    Uses gen_assert_json_str which parses both sides before comparing,
    tolerating key-ordering differences.
    """
    json_expr = f"serde_json::json!({json.dumps(value)})"
    is_option = field_lang_type is not None and field_lang_type.startswith("Option<")
    if is_option:
        return f"gen_assert_json_str({field_expr}.as_deref().unwrap(), {json_expr});"
    return f"gen_assert_json_str(&{field_expr}, {json_expr});"


def render_assert_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Null assertion.

    Bare variables (result pointers) use .is_none().
    Struct fields use is_none() as well for uniformity.
    """
    return f"assert!({expr}.is_none());"


def render_assert_not_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Not-null assertion.

    For bare variables (Option<T> result): rebinds by unwrapping with expect.
    For struct fields: asserts is_some().
    """
    if "." not in expr:
        return f'let {expr} = {expr}.expect("expected non-nil {expr}");'
    return f"assert!({expr}.is_some());"


def render_assert_null(expr: str) -> str:
    """Fallback null assertion."""
    return f"assert!({expr}.is_none());"


def render_assert_not_null(expr: str) -> str:
    """Fallback not-null assertion."""
    if "." not in expr:
        return f'let {expr} = {expr}.expect("expected non-nil {expr}");'
    return f"assert!({expr}.is_some());"


def render_assert_len(var_expr: str, length: str) -> str:
    """Length assertion."""
    return f"assert_eq!({var_expr}.len(), {length});"


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """UUID string comparison."""
    return f"assert_eq!({field_expr}, {var_name});"


# ── Internal helpers ─────────────────────────────────────────────────────


_RUST_BUILTIN_TYPES = frozenset({
    "String", "i8", "i16", "i32", "i64", "i128",
    "u8", "u16", "u32", "u64", "u128",
    "f32", "f64", "bool",
})

_RUST_KNOWN_PREFIXES = ("Vec<", "Option<", "serde_json::", "time::", "sqlx::", "uuid::", "rust_decimal::")


def _is_enum_type(lang_type: str) -> bool:
    """Return True if lang_type looks like a generated Rust enum type.

    Rust enums are PascalCase identifiers (e.g. Priority, Status).
    Builtins (String, i64, etc.) and library types are not enums.
    """
    if lang_type in _RUST_BUILTIN_TYPES:
        return False
    if any(lang_type.startswith(p) for p in _RUST_KNOWN_PREFIXES):
        return False
    # Must be PascalCase: starts with uppercase letter, no colons/brackets
    return bool(lang_type) and lang_type[0].isupper() and "::" not in lang_type and "<" not in lang_type


def _escape_str(s: str) -> str:
    """Escape a string for use in a Rust string literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _rust_json_value(value: Any, coercions: dict[str, str]) -> str:
    """Render a JSON value as a Rust expression for render_value (let steps)."""
    if coercions.get("json") == "json_string":
        return f"serde_json::to_string(&serde_json::json!({json.dumps(value)})).unwrap()"
    return f"serde_json::json!({json.dumps(value)})"


def _rust_json_for_type(inner_type: str, value: Any) -> str:
    """Render a JSON value for a specific inner lang_type."""
    if "String" in inner_type:
        return f"serde_json::to_string(&serde_json::json!({json.dumps(value)})).unwrap()"
    return f"serde_json::json!({json.dumps(value)})"


def _rust_uuid_value(value: Any, coercions: dict[str, str]) -> str:
    """Render a UUID value as Rust expression for let steps."""
    if value == "random":
        if coercions.get("uuid") == "string":
            return "uuid::Uuid::new_v4().to_string()"
        return "uuid::Uuid::new_v4()"
    if coercions.get("uuid") == "string":
        return f'"{value}".to_string()'
    return f'uuid::Uuid::parse_str("{value}").unwrap()'


def _rust_uuid_for_type(inner_type: str, value: Any) -> str:
    """Render a UUID value for a specific inner lang_type."""
    if value == "random":
        if "String" in inner_type:
            return "uuid::Uuid::new_v4().to_string()"
        return "uuid::Uuid::new_v4()"
    if "String" in inner_type:
        return f'"{value}".to_string()'
    return f'uuid::Uuid::parse_str("{value}").unwrap()'


def _rust_primitive_datetime(s: str) -> str:
    """Parse a datetime string and return a Rust datetime!() macro expression (PrimitiveDateTime)."""
    s_clean = s.rstrip("Z").replace("T", " ")
    return f"datetime!({s_clean})"


def _rust_offset_datetime(s: str) -> str:
    """Parse a datetime string and return a Rust datetime!() macro expression (OffsetDateTime)."""
    s_clean = s.rstrip("Z").replace("T", " ")
    return f"datetime!({s_clean} UTC)"


def _rust_datetime_for_type(inner_type: str, s: str) -> str:
    """Render a datetime value for a specific inner lang_type."""
    has_tz = s.endswith("Z") or "+" in s[10:]
    if "OffsetDateTime" in inner_type and has_tz:
        return _rust_offset_datetime(s)
    return _rust_primitive_datetime(s)


def _rust_date(s: str) -> str:
    """Parse a date string (YYYY-MM-DD) and return a Rust date!() macro expression."""
    return f"date!({s})"


def _rust_time(s: str) -> str:
    """Parse a time string (HH:MM:SS) and return a Rust time!() macro expression."""
    return f"time!({s})"
