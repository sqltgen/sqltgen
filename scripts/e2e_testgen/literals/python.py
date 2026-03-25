"""Python literal renderers for abstract typed values.

Each function returns a Python source code string for the given abstract value.
The `engine_coercions` dict maps abstract types to coerced forms (e.g. sqlite
coerces datetime → string).
"""

from __future__ import annotations

import json
from typing import Any


def render_value(kind: str, value: Any, engine: str, coercions: dict[str, str]) -> str:
    """Render an abstract typed value as a Python literal string."""
    # Apply engine coercion if applicable
    coerced = coercions.get(kind)
    if coerced:
        return _render_coerced(kind, value, coerced)

    renderers = {
        "str": _render_str,
        "int": _render_int,
        "float": _render_float,
        "bool": _render_bool,
        "null": _render_null,
        "json": _render_json,
        "uuid": _render_uuid,
        "datetime": _render_datetime,
        "date": _render_date,
        "time": _render_time,
        "var": _render_var,
    }
    renderer = renderers.get(kind)
    if renderer is None:
        raise ValueError(f"Unknown value type: {kind}")
    return renderer(value)


def render_assert_eq(field_expr: str, expected: str) -> str:
    """Render an equality assertion."""
    return f"assert {field_expr} == {expected}"


def render_assert_json_eq(field_expr: str, value: Any) -> str:
    """Render a JSON equality assertion that parses the field before comparing.

    Used when the engine coerces JSON to a string on round-trip (e.g. MySQL),
    where key ordering may differ from the original serialized form.
    """
    return f"assert json.loads({field_expr}) == {repr(value)}"


def render_assert_null(expr: str) -> str:
    """Render a null assertion."""
    return f"assert {expr} is None"


def render_assert_not_null(expr: str) -> str:
    """Render a not-null assertion."""
    return f"assert {expr} is not None"


def render_assert_len(var_expr: str, length: str) -> str:
    """Render a length assertion."""
    return f"assert len({var_expr}) == {length}"


def render_call(func_name: str, args: list[str], bind: str | None = None) -> str:
    """Render a function call with optional binding."""
    args_str = ", ".join(["conn"] + args)
    call = f"queries.{func_name}({args_str})"
    if bind:
        return f"{bind} = {call}"
    return call


def render_uuid_compare(field_expr: str, var_name: str) -> str:
    """Render a UUID string comparison (str(field) == var)."""
    return f"assert str({field_expr}) == {var_name}"


def null_literal() -> str:
    return "None"


def conn_param() -> str:
    return "conn"


# ── Internal renderers ──────────────────────────────────────────────────


def _render_str(value: Any) -> str:
    return repr(str(value))


def _render_int(value: Any) -> str:
    return str(int(value))


def _render_float(value: Any) -> str:
    return str(float(value))


def _render_bool(value: Any) -> str:
    return "True" if value else "False"


def _render_null(_: Any) -> str:
    return "None"


def _render_json(value: Any) -> str:
    """Render a JSON value as a Python dict/list literal."""
    return repr(value)


def _render_uuid(value: Any) -> str:
    if value == "random":
        return "str(uuid.uuid4())"
    return repr(str(value))


def _render_datetime(value: Any) -> str:
    """Render a datetime from ISO string to Python datetime constructor."""
    s = str(value)
    has_tz = s.endswith("Z") or "+" in s[10:]
    s_clean = s.rstrip("Z")

    parts = s_clean.replace("T", "-").replace(":", "-").split("-")
    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    hour = int(parts[3]) if len(parts) > 3 else 0
    minute = int(parts[4]) if len(parts) > 4 else 0
    second = int(parts[5]) if len(parts) > 5 else 0

    base = f"datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}"
    if has_tz:
        base += ", tzinfo=datetime.timezone.utc"
    return base + ")"


def _render_date(value: Any) -> str:
    parts = str(value).split("-")
    return f"datetime.date({int(parts[0])}, {int(parts[1])}, {int(parts[2])})"


def _render_time(value: Any) -> str:
    parts = str(value).split(":")
    return f"datetime.time({int(parts[0])}, {int(parts[1])}, {int(parts[2])})"


def _render_var(value: Any) -> str:
    return str(value)


def _render_coerced(kind: str, value: Any, coercion: str) -> str:
    """Render a value with an engine-specific type coercion applied."""
    if coercion == "string":
        if kind == "datetime":
            # SQLite: datetime as ISO string
            s = str(value)
            if s.endswith("Z"):
                s = s[:-1].replace("T", " ")
            elif "T" in s:
                s = s.replace("T", " ")
            return repr(s)
        elif kind == "date":
            return repr(str(value))
        elif kind == "time":
            return repr(str(value))
        elif kind == "uuid":
            if value == "random":
                return "str(uuid.uuid4())"
            return repr(str(value))
        else:
            return repr(str(value))
    elif coercion == "json_string":
        # SQLite/MySQL: JSON as pre-serialized string
        return f"json.dumps({repr(value)})"
    elif coercion == "naive_datetime":
        # MySQL DATETIME has no timezone; strip tzinfo from both args and assertions.
        s = str(value).rstrip("Z")
        parts = s.replace("T", "-").replace(":", "-").split("-")
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        hour = int(parts[3]) if len(parts) > 3 else 0
        minute = int(parts[4]) if len(parts) > 4 else 0
        second = int(parts[5]) if len(parts) > 5 else 0
        return f"datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second})"
    else:
        raise ValueError(f"Unknown coercion: {coercion}")
