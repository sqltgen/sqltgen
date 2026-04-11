"""Kotlin literal renderers for abstract typed values.

Implements the extended generator protocol for Kotlin's static type system:
  - render_typed_arg: uses manifest lang_type for type-correct arg construction
  - render_call_lines: wraps :one calls with !! force-unwrap or keeps nullable
  - transform_field_expr: converts field expressions to Kotlin property syntax
  - render_assert_eq_typed: type-aware assertions (Long suffix, OffsetDateTime, JSON)
  - render_assert_null_typed: assertNull for all expressions (no Optional in Kotlin)
  - render_assert_not_null_typed: assertNotNull(expr)
"""

from __future__ import annotations

import json as _json
import re
from typing import Any

from literals.jvm_helpers import (
    jvm_local_date,
    jvm_local_datetime,
    jvm_local_time,
    jvm_offset_datetime,
    jvm_str,
    strip_java_time,
)


# Known Kotlin types that are NOT enums.
_KOTLIN_KNOWN_TYPES = frozenset([
    "String", "Int", "Long", "Short", "Float", "Double", "Boolean", "Byte",
    "Any", "BigDecimal", "UUID",
    "java.math.BigDecimal", "java.util.UUID",
])


def _is_enum_type(lang_type: str) -> bool:
    """Return True if lang_type looks like a generated enum type name."""
    if not lang_type or not lang_type[0].isupper():
        return False
    # Strip nullable wrapper if present
    inner = lang_type.rstrip("?")
    # Known Kotlin types are not enums
    if inner in _KOTLIN_KNOWN_TYPES:
        return False
    # Contains a dot -> FQN like java.time.LocalDate, not an enum
    if "." in inner:
        return False
    # Starts with uppercase, not a known type -> likely an enum
    return True


# ── Protocol: optional extension methods ────────────────────────────────

def step_indent() -> str:
    """Kotlin method bodies inside a class need 8-space indentation (2 levels)."""
    return "        "


def stmt_terminator() -> str:
    """Kotlin statements do not end with a semicolon."""
    return ""


def assign_op() -> str:
    return "="


def decl_prefix() -> str:
    return "val "


def func_prefix() -> str:
    """Generated query functions are static methods on the Queries object."""
    return "Queries."


def conn_param() -> str:
    """Kotlin generated functions take conn as the first parameter."""
    return "conn"


def null_literal() -> str:
    return "null"


# ── transform_field_expr ─────────────────────────────────────────────────

def transform_field_expr(expr: str) -> str:
    """Convert field expressions to Kotlin property access syntax.

    ev.name       → ev.name     (property, no parentheses)
    ev.doc_id     → ev.docId    (camelCase conversion, no parentheses)
    events[0].name → events[0].name  (Kotlin uses [] natively, no .get(N))
    n             → n           (bare variable, unchanged)
    """
    if '.' not in expr:
        return expr  # bare variable, no transformation

    parts = expr.split('.')
    result = [parts[0]]  # receiver variable unchanged
    for part in parts[1:]:
        if '(' in part or '[' in part:
            # Already a method call or index access — leave as-is
            result.append(part)
        else:
            result.append(_to_camel_case(part))
    return '.'.join(result)


# ── render_value: for let-step values and assertion expected values ──────

def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract value as a Kotlin expression."""
    if kind == "str":
        return _kotlin_str(str(value))
    elif kind == "int":
        return str(int(value))
    elif kind == "float":
        return str(float(value))
    elif kind == "bool":
        return "true" if value else "false"
    elif kind == "null":
        return "null"
    elif kind == "json":
        if coercions.get("json") == "json_string":
            return _kotlin_str(_json.dumps(value))
        return f'genJson({_kotlin_str(_json.dumps(value))})'
    elif kind == "uuid":
        if value == "random":
            if coercions.get("uuid") == "string":
                return "UUID.randomUUID().toString()"
            return "UUID.randomUUID()"
        if coercions.get("uuid") == "string":
            return _kotlin_str(str(value))
        return f'UUID.fromString({_kotlin_str(str(value))})'
    elif kind == "datetime":
        return _kotlin_local_datetime(str(value))
    elif kind == "date":
        return _kotlin_local_date(str(value))
    elif kind == "time":
        return _kotlin_local_time(str(value))
    elif kind == "list":
        if not value:
            return "emptyList()"
        parts = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            parts.append(render_value(str(item_kind), item_val, engine, coercions))
        return f"listOf({', '.join(parts)})"
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
    """Render a call argument using its exact Kotlin lang_type from the manifest."""
    inner = _strip_java_time(lang_type)

    if kind == "null":
        return "null"

    if kind == "list":
        m = re.match(r"List<(.+)>", lang_type)
        elem_lang_type = m.group(1) if m else ""
        if not value:
            stripped_elem = _strip_java_time(elem_lang_type)
            return f"emptyList<{stripped_elem}>()"
        elements = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            elements.append(
                render_typed_arg("_", elem_lang_type, str(item_kind), item_val, engine, coercions)
            )
        return f"listOf({', '.join(elements)})"

    if kind == "json":
        if "String" in inner:
            return _kotlin_str(_json.dumps(value))
        return f'genJson({_kotlin_str(_json.dumps(value))})'

    if kind == "datetime":
        if coercions.get("datetime") == "string":
            return _kotlin_str(str(value))
        if "OffsetDateTime" in inner:
            return _kotlin_offset_datetime(str(value))
        return _kotlin_local_datetime(str(value))

    if kind == "date":
        if coercions.get("date") == "string":
            return _kotlin_str(str(value))
        return _kotlin_local_date(str(value))

    if kind == "time":
        if coercions.get("time") == "string":
            return _kotlin_str(str(value))
        return _kotlin_local_time(str(value))

    if kind == "uuid":
        if "String" in inner:
            if value == "random":
                return "UUID.randomUUID().toString()"
            return _kotlin_str(str(value))
        if value == "random":
            return "UUID.randomUUID()"
        return f'UUID.fromString({_kotlin_str(str(value))})'

    if kind == "str":
        if _is_enum_type(lang_type):
            enum_name = lang_type.rstrip("?")
            return f'{enum_name}.fromValue({_kotlin_str(str(value))})'
        return _kotlin_str(str(value))

    if kind == "int":
        if "Long" in inner:
            return f"{int(value)}L"
        return str(int(value))

    if kind == "float":
        if "BigDecimal" in inner:
            return f'java.math.BigDecimal("{value}")'
        return str(float(value))

    if kind == "var":
        return str(value)

    return render_value(kind, value, engine, coercions)


# ── render_call_lines: Kotlin-style with nullable T? handling for :one ──

def render_call_lines(
    call_expr: str,
    bind: str | None,
    command: str,
    indent: str,
    null_checked_vars: set[str] | None = None,
) -> list[str]:
    """Render a function call as Kotlin source lines.

    For :one with bind, uses !! force-unwrap unless the bind variable is in
    null_checked_vars (meaning a subsequent assert_null step checks it), in
    which case the nullable T? is kept so assertNull can be used.
    """
    if null_checked_vars is None:
        null_checked_vars = set()

    if bind:
        if command == "one":
            if bind in null_checked_vars:
                return [f"{indent}val {bind} = {call_expr}"]
            return [f"{indent}val {bind} = {call_expr}!!"]
        return [f"{indent}val {bind} = {call_expr}"]
    return [f"{indent}{call_expr}"]


# ── Assertion renderers ───────────────────────────────────────────────────

def render_assert_eq(field_expr: str, expected: str) -> str:
    """Fallback equality assertion (JUnit5 assertEquals with expected first)."""
    return f"assertEquals({expected}, {field_expr})"


def render_assert_eq_typed(
    field_expr: str,
    expected: str,
    kind: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Type-aware equality assertion.

    For Long fields: appends L suffix so JUnit5 uses the long overload.
    For datetime with OffsetDateTime field: upgrades expected to OffsetDateTime.
    For json: expected is already genJson(...) so plain assertEquals works.
    """
    inner = _strip_java_time(field_lang_type or "")

    if kind == "int" and ("Long" in inner or not inner):
        return f"assertEquals({expected}L, {field_expr})"

    if kind == "datetime" and "OffsetDateTime" in inner:
        m = re.match(r'LocalDateTime\.of\((.+)\)', expected)
        if m:
            args = m.group(1)
            ot_expr = f"OffsetDateTime.of({args}, 0, ZoneOffset.UTC)"
            return f"assertEquals({ot_expr}, {field_expr})"

    if kind == "str" and _is_enum_type(field_lang_type or ""):
        enum_name = (field_lang_type or "").rstrip("?")
        return f"assertEquals({enum_name}.fromValue({expected}), {field_expr})"

    return f"assertEquals({expected}, {field_expr})"


def render_assert_json_eq(field_expr: str, value: Any, field_lang_type: str | None = None) -> str:
    """JSON equality assertion for json_string coercion (SQLite).

    Uses genJson() to parse both sides before comparing, tolerating key-ordering
    differences in serialised JSON strings. Adds !! for nullable String? fields.
    """
    json_str = _kotlin_str(_json.dumps(value))
    inner = _strip_java_time(field_lang_type or "")
    if "String" in inner:
        nullable = inner.endswith("?") or "?" in inner
        unwrap = "!!" if nullable else ""
        return f"assertEquals(genJson({json_str}), genJson({field_expr}{unwrap}))"
    # Fallback: field is already JsonNode (or JsonNode?)
    nullable = inner.endswith("?") or "?" in inner
    unwrap = "!!" if nullable else ""
    return f"assertEquals(genJson({json_str}), {field_expr}{unwrap})"


def render_assert_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Null assertion. Kotlin uses assertNull for both bare vars and fields."""
    return f"assertNull({expr})"


def render_assert_not_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Not-null assertion."""
    return f"assertNotNull({expr})"


def render_assert_null(expr: str) -> str:
    """Fallback null assertion."""
    return f"assertNull({expr})"


def render_assert_not_null(expr: str) -> str:
    """Fallback not-null assertion."""
    return f"assertNotNull({expr})"


def render_assert_len(var_expr: str, length: str) -> str:
    """Length assertion using Kotlin List.size property."""
    return f"assertEquals({length}, {var_expr}.size)"


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """UUID equality assertion (works for both UUID and String types)."""
    return f"assertEquals({var_name}, {field_expr})"


# ── Internal helpers ─────────────────────────────────────────────────────


def _kotlin_str(s: object) -> str:
    return jvm_str(s)


def _strip_java_time(lang_type: str) -> str:
    return strip_java_time(lang_type)


def _to_camel_case(s: str) -> str:
    """Convert snake_case to camelCase."""
    if '_' not in s:
        return s
    parts = s.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])


def _kotlin_local_datetime(s: str) -> str:
    return jvm_local_datetime(s)


def _kotlin_offset_datetime(s: str) -> str:
    return jvm_offset_datetime(s)


def _kotlin_local_date(s: str) -> str:
    return jvm_local_date(s)


def _kotlin_local_time(s: str) -> str:
    return jvm_local_time(s)
