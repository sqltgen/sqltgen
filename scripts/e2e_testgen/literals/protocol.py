"""Renderer protocol: the interface every literal renderer module must implement.

Each entry in REQUIRED is called unconditionally by codegen.py; a missing
method produces a silent wrong-output bug rather than a traceback.  The
validate_renderer() guard is called at load time so the failure is loud and
immediate.

Optional methods (those checked with hasattr() in codegen.py) are documented
here for reference but are not enforced — their absence is deliberate and the
caller provides a fallback.

Required
--------
conn_param() -> str
render_value(kind, value, engine, coercions) -> str
render_assert_eq(field_expr, expected) -> str
render_assert_null(expr) -> str
render_assert_not_null(expr) -> str
render_assert_json_eq(field_expr, value, field_lang_type) -> str
render_assert_len(var_expr, length) -> str
render_uuid_compare(field_expr, var_name) -> str

Optional (hasattr-guarded, caller provides fallback or skips)
-------------------------------------------------------------
step_indent() -> str
assign_op() -> str
decl_prefix() -> str
use_await() -> bool
func_prefix() -> str
stmt_terminator() -> str
null_literal() -> str
transform_field_expr(expr) -> str
render_typed_arg(arg_name, lang_type, kind, value, engine, coercions) -> str
render_call_lines(call_expr, bind, command, indent, null_checked_vars) -> list[str]
render_assert_eq_typed(field_expr, expected, kind, engine, coercions, field_lang_type) -> str
render_assert_null_typed(expr, engine, coercions, field_lang_type) -> str
render_assert_not_null_typed(expr, engine, coercions, field_lang_type) -> str
"""

from __future__ import annotations

from types import ModuleType

REQUIRED: list[str] = [
    "conn_param",
    "render_value",
    "render_assert_eq",
    "render_assert_null",
    "render_assert_not_null",
    "render_assert_json_eq",
    "render_assert_len",
    "render_uuid_compare",
]


def validate_renderer(mod: ModuleType) -> None:
    """Raise NotImplementedError if any required method is absent from mod."""
    missing = [m for m in REQUIRED if not callable(getattr(mod, m, None))]
    if missing:
        raise NotImplementedError(
            f"{mod.__name__} is missing required renderer methods: {', '.join(missing)}"
        )
