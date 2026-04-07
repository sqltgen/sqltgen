"""Java literal renderers for abstract typed values.

Implements the extended generator protocol for Java's static type system:
  - render_typed_arg: uses manifest lang_type for type-correct arg construction
  - render_call_lines: wraps :one calls with .orElseThrow() or keeps Optional
  - transform_field_expr: converts field expressions to Java accessor syntax
  - render_assert_eq_typed: type-aware assertions (long suffix, OffsetDateTime, JSON)
  - render_assert_null_typed: assertTrue(.isEmpty()) for Optional, assertNull for fields
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


# Known Java/JDBC types that are NOT enums.
_JAVA_KNOWN_TYPES = frozenset([
    "String", "int", "long", "short", "float", "double", "boolean", "byte",
    "Integer", "Long", "Short", "Float", "Double", "Boolean", "Byte",
    "Object", "BigDecimal", "UUID",
    "java.math.BigDecimal", "java.util.UUID",
])


def _is_enum_type(lang_type: str) -> bool:
    """Return True if lang_type looks like a generated enum type name."""
    if not lang_type or not lang_type[0].isupper():
        return False
    # Strip nullable wrapper if present
    inner = lang_type.rstrip("?")
    # Known Java types are not enums
    if inner in _JAVA_KNOWN_TYPES:
        return False
    # Contains a dot → FQN like java.time.LocalDate, not an enum
    if "." in inner:
        return False
    # Starts with uppercase, not a known type → likely an enum
    return True


# ── Protocol: optional extension methods ────────────────────────────────

def step_indent() -> str:
    """Java method bodies inside a class need 8-space indentation (2 levels)."""
    return "        "


def stmt_terminator() -> str:
    """Java statements end with a semicolon."""
    return ";"


def assign_op() -> str:
    return "="


def decl_prefix() -> str:
    return "var "


def func_prefix() -> str:
    """Generated query functions are static methods on the Queries class."""
    return "Queries."


def conn_param() -> str:
    """Java generated functions take conn as the first parameter."""
    return "conn"


def null_literal() -> str:
    return "null"


# ── transform_field_expr ─────────────────────────────────────────────────

def transform_field_expr(expr: str) -> str:
    """Convert field expressions to Java accessor syntax.

    ev.name       → ev.name()
    ev.doc_id     → ev.docId()
    events[0].name → events.get(0).name()
    n             → n  (bare variable, unchanged)
    """
    # Replace array indexing with .get(N)
    expr = re.sub(r'\[(\d+)\]', lambda m: f'.get({m.group(1)})', expr)

    if '.' not in expr:
        return expr  # bare variable, no transformation

    parts = expr.split('.')
    result = [parts[0]]  # receiver variable unchanged
    for part in parts[1:]:
        if '(' in part:
            # Already a method call (e.g., get(0)) — leave as-is
            result.append(part)
        else:
            result.append(_to_camel_case(part) + '()')
    return '.'.join(result)


# ── render_value: for let-step values and assertion expected values ──────

def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract value as a Java expression.

    For datetime/date/time, always uses java.time types regardless of coercions —
    the Java JDBC driver handles string storage internally. UUID and JSON respect
    coercions since those affect the actual Java API surface.
    """
    if kind == "str":
        return _java_str(str(value))
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
            return _java_str(_json.dumps(value))
        return f'genJson({_java_str(_json.dumps(value))})'
    elif kind == "uuid":
        if value == "random":
            if coercions.get("uuid") == "string":
                return "UUID.randomUUID().toString()"
            return "UUID.randomUUID()"
        if coercions.get("uuid") == "string":
            return _java_str(str(value))
        return f'UUID.fromString({_java_str(str(value))})'
    elif kind == "datetime":
        return _java_local_datetime(str(value))
    elif kind == "date":
        return _java_local_date(str(value))
    elif kind == "time":
        return _java_local_time(str(value))
    elif kind == "list":
        if not value:
            return "List.of()"
        parts = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            parts.append(render_value(str(item_kind), item_val, engine, coercions))
        return f"List.of({', '.join(parts)})"
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
    """Render a call argument using its exact Java lang_type from the manifest."""
    inner = _strip_java_time(lang_type)

    if kind == "null":
        return "null"

    if kind == "list":
        m = re.match(r"java\.util\.List<(.+)>", lang_type)
        elem_lang_type = m.group(1) if m else ""
        if not value:
            return "List.of()"
        elements = []
        for item in value:
            item_kind, item_val = next(iter(item.items()))
            if item_kind is None:
                item_kind = "null"
            elements.append(
                render_typed_arg("_", elem_lang_type, str(item_kind), item_val, engine, coercions)
            )
        return f"List.of({', '.join(elements)})"

    if kind == "json":
        if "String" in inner:
            return _java_str(_json.dumps(value))
        return f'genJson({_java_str(_json.dumps(value))})'

    if kind == "datetime":
        if "OffsetDateTime" in inner:
            return _java_offset_datetime(str(value))
        return _java_local_datetime(str(value))

    if kind == "date":
        return _java_local_date(str(value))

    if kind == "time":
        return _java_local_time(str(value))

    if kind == "uuid":
        if "String" in inner:
            if value == "random":
                return "UUID.randomUUID().toString()"
            return _java_str(str(value))
        if value == "random":
            return "UUID.randomUUID()"
        return f'UUID.fromString({_java_str(str(value))})'

    if kind == "str":
        if _is_enum_type(lang_type):
            return f'{lang_type}.fromValue({_java_str(str(value))})'
        return _java_str(str(value))

    if kind == "int":
        return str(int(value))

    if kind == "var":
        return str(value)

    return render_value(kind, value, engine, coercions)


# ── render_call_lines: Java-style with Optional handling for :one ────────

def render_call_lines(
    call_expr: str,
    bind: str | None,
    command: str,
    indent: str,
    null_checked_vars: set[str] | None = None,
) -> list[str]:
    """Render a function call as Java source lines.

    For :one with bind, uses .orElseThrow() unless the bind variable is in
    null_checked_vars (meaning a subsequent assert_null step checks it), in
    which case the Optional is kept so isEmpty() can be asserted.
    """
    if null_checked_vars is None:
        null_checked_vars = set()

    if bind:
        if command == "one":
            if bind in null_checked_vars:
                return [f"{indent}var {bind} = {call_expr};"]
            return [f"{indent}var {bind} = {call_expr}.orElseThrow();"]
        return [f"{indent}var {bind} = {call_expr};"]
    return [f"{indent}{call_expr};"]


# ── Assertion renderers ───────────────────────────────────────────────────

def render_assert_eq(field_expr: str, expected: str) -> str:
    """Fallback equality assertion (JUnit5 assertEquals with expected first)."""
    return f"assertEquals({expected}, {field_expr});"


def render_assert_eq_typed(
    field_expr: str,
    expected: str,
    kind: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Type-aware equality assertion.

    For int: appends L suffix so JUnit5 uses the long overload.
    For datetime with OffsetDateTime field: upgrades expected to OffsetDateTime.
    For json: expected is already genJson(...) so plain assertEquals works.
    """
    inner = _strip_java_time(field_lang_type or "")

    if kind == "int":
        return f"assertEquals({expected}L, {field_expr});"

    if kind == "datetime" and "OffsetDateTime" in inner:
        m = re.match(r'LocalDateTime\.of\((.+)\)', expected)
        if m:
            args = m.group(1)
            ot_expr = f"OffsetDateTime.of({args}, 0, ZoneOffset.UTC)"
            return f"assertEquals({ot_expr}, {field_expr});"

    if kind == "str" and _is_enum_type(field_lang_type or ""):
        enum_type = field_lang_type
        return f"assertEquals({enum_type}.fromValue({expected}), {field_expr});"

    return f"assertEquals({expected}, {field_expr});"


def render_assert_json_eq(field_expr: str, value: Any, field_lang_type: str | None = None) -> str:
    """JSON equality assertion for json_string coercion (SQLite).

    Uses genJson() to parse both sides before comparing, tolerating key-ordering
    differences in serialised JSON strings.
    """
    json_str = _java_str(_json.dumps(value))
    inner = _strip_java_time(field_lang_type or "")
    if "String" in inner:
        return f"assertEquals(genJson({json_str}), genJson({field_expr}));"
    # Fallback: field is already JsonNode
    return f"assertEquals(genJson({json_str}), {field_expr});"


def render_assert_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Null assertion.

    Bare variables from :one calls are Optional<T>; use isEmpty().
    Struct fields use assertNull.
    """
    if '.' not in expr:
        return f"assertTrue({expr}.isEmpty());"
    return f"assertNull({expr});"


def render_assert_not_null_typed(
    expr: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Not-null assertion.

    For vars already unwrapped via orElseThrow(), assertNotNull is trivially true
    but documents intent. For field accesses, it asserts the field is non-null.
    """
    return f"assertNotNull({expr});"


def render_assert_null(expr: str) -> str:
    """Fallback null assertion."""
    if '.' not in expr:
        return f"assertTrue({expr}.isEmpty());"
    return f"assertNull({expr});"


def render_assert_not_null(expr: str) -> str:
    """Fallback not-null assertion."""
    return f"assertNotNull({expr});"


def render_assert_len(var_expr: str, length: str) -> str:
    """Length assertion using Java List.size()."""
    return f"assertEquals({length}, {var_expr}.size());"


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """UUID equality assertion (works for both UUID and String types)."""
    return f"assertEquals({var_name}, {field_expr});"


# ── Internal helpers ─────────────────────────────────────────────────────


def _java_str(s: object) -> str:
    return jvm_str(s)


def _strip_java_time(lang_type: str) -> str:
    return strip_java_time(lang_type)


def _to_camel_case(s: str) -> str:
    """Convert snake_case to camelCase."""
    if '_' not in s:
        return s
    parts = s.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])


def _java_local_datetime(s: str) -> str:
    return jvm_local_datetime(s)


def _java_offset_datetime(s: str) -> str:
    return jvm_offset_datetime(s)


def _java_local_date(s: str) -> str:
    return jvm_local_date(s)


def _java_local_time(s: str) -> str:
    return jvm_local_time(s)
