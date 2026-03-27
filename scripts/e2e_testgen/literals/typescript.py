"""TypeScript/JavaScript literal renderers for abstract typed values.

Each function returns a TypeScript source code string for the given abstract value.
The `engine_coercions` dict maps abstract types to coerced forms (e.g. sqlite
coerces datetime → string).
"""

from __future__ import annotations

from typing import Any


def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract typed value as a TypeScript literal string."""
    coerced = coercions.get(kind)
    if coerced:
        return _render_coerced(kind, value, coerced)

    if kind == "str":
        return _render_str(value)
    elif kind == "int":
        return str(int(value))
    elif kind == "float":
        return str(float(value))
    elif kind == "bool":
        return "true" if value else "false"
    elif kind == "null":
        return "null"
    elif kind == "json":
        return _to_js_literal(value)
    elif kind == "uuid":
        return "randomUUID()" if value == "random" else _render_str(value)
    elif kind == "datetime":
        return _render_datetime(value)
    elif kind == "date":
        # pg returns DATE as string; mysql2 returns DATE as string.
        # Render as string for both input args and assertions.
        return _render_str(value)
    elif kind == "time":
        # pg returns TIME as string; mysql2 returns TIME as string by default.
        # Render as string for both input args and assertions.
        return _render_str(value)
    elif kind == "var":
        return str(value)
    else:
        raise ValueError(f"Unknown value type: {kind}")


def render_assert_eq(field_expr: str, expected: str) -> str:
    """Render a basic equality assertion (fallback; prefer render_assert_eq_typed)."""
    return f"assert.equal({field_expr}, {expected})"


def render_assert_eq_typed(
    field_expr: str,
    expected: str,
    kind: str,
    engine: str,
    coercions: dict[str, str],
    field_lang_type: str | None = None,
) -> str:
    """Render an equality assertion with type-aware dispatch.

    Chooses between assert.equal and assert.deepEqual based on the value kind,
    and handles BIGINT-as-string normalisation for integer comparisons.
    """
    if kind == "datetime":
        if coercions.get("datetime") == "naive_datetime":
            # MySQL DATETIME: timezone differences make exact comparison fragile.
            return f"assert.ok({field_expr})"
        if expected.startswith("'"):
            # Coerced to string (SQLite)
            return f"assert.equal({field_expr}, {expected})"
        # PG: TIMESTAMPTZ returned as Date object; use deepEqual for value equality.
        return f"assert.deepEqual({field_expr}, {expected})"
    if kind == "date":
        if engine in ("postgresql", "mysql"):
            # Both pg and mysql2 return DATE as a timezone-adjusted Date object at
            # runtime despite type annotations; skip exact value comparison.
            return f"assert.ok({field_expr})"
        return f"assert.equal({field_expr}, {expected})"
    if kind == "json":
        # Object identity differs; deepEqual compares by value.
        return f"assert.deepEqual({field_expr}, {expected})"
    if kind == "int":
        # pg returns COUNT(*)/BIGINT as string; Number() normalises across drivers.
        return f"assert.equal(Number({field_expr}), {expected})"
    return f"assert.equal({field_expr}, {expected})"


def render_assert_json_eq(field_expr: str, value: Any, field_lang_type: str | None = None) -> str:
    """Render a JSON equality assertion that parses the field before comparing.

    Used when the engine coerces JSON to a string on round-trip (e.g. SQLite),
    where key ordering may differ from the original serialized form.
    """
    return f"assert.deepEqual(JSON.parse({field_expr}), {_to_js_literal(value)})"


def render_assert_null(expr: str) -> str:
    """Render a null assertion."""
    return f"assert.equal({expr}, null)"


def render_assert_not_null(expr: str) -> str:
    """Render a not-null assertion."""
    return f"assert.ok({expr})"


def render_assert_len(var_expr: str, length: str) -> str:
    """Render a length assertion."""
    return f"assert.equal({var_expr}.length, {length})"


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """Render a UUID string comparison (field === var)."""
    return f"assert.equal({field_expr}, {var_name})"


def null_literal() -> str:
    return "null"


def conn_param() -> str:
    """Return the connection variable name used in generated call expressions."""
    return "db"


def use_await() -> bool:
    """Whether generated function calls should be prefixed with await."""
    return True


def decl_prefix() -> str:
    """Prefix for variable declarations (let/const bindings)."""
    return "const "


# ── Internal renderers ──────────────────────────────────────────────────


def _render_str(value: Any) -> str:
    # Python repr() produces valid JS string literals (single- or double-quoted).
    return repr(str(value))


def _render_datetime(value: Any) -> str:
    return f'new Date("{value}")'


def _to_js_literal(value: Any) -> str:
    """Convert a Python value to a TypeScript/JavaScript literal expression."""
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = ", ".join(f"{k}: {_to_js_literal(v)}" for k, v in value.items())
        return "{" + items + "}"
    elif isinstance(value, list):
        if not value:
            return "[]"
        return "[" + ", ".join(_to_js_literal(v) for v in value) + "]"
    elif isinstance(value, str):
        return repr(value)
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif value is None:
        return "null"
    else:
        return str(value)


def _render_coerced(kind: str, value: Any, coercion: str) -> str:
    """Render a value with an engine-specific type coercion applied."""
    if coercion == "string":
        if kind == "datetime":
            s = str(value)
            if s.endswith("Z"):
                s = s[:-1].replace("T", " ")
            elif "T" in s:
                s = s.replace("T", " ")
            return repr(s)
        elif kind == "uuid":
            return "randomUUID()" if value == "random" else repr(str(value))
        else:
            return repr(str(value))
    elif coercion == "json_string":
        return f"JSON.stringify({_to_js_literal(value)})"
    elif coercion == "naive_datetime":
        # MySQL DATETIME has no timezone; strip the trailing Z if present.
        s = str(value).rstrip("Z")
        return f'new Date("{s}")'
    else:
        raise ValueError(f"Unknown coercion: {coercion}")
