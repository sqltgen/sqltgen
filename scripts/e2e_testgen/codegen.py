"""Render language-specific test files from spec + manifest."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import jinja2

from manifest import Manifest
from test_spec import EngineOverride, Scenario, Step, TestSpec, TypedValue


TEMPLATES_DIR = Path(__file__).parent / "templates"


def render_test(
    language: str,
    engine: str,
    spec: TestSpec,
    manifest: Manifest,
    engine_override: EngineOverride,
) -> str:
    """Render a test file for a (language, engine) combination."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["to_pascal_case"] = lambda s: "".join(w.capitalize() for w in s.split("_"))

    # Load the language-specific literal renderer
    literals_mod = _load_literals(language)

    # Pre-render all scenario bodies
    rendered_scenarios = []
    for scenario in spec.scenarios:
        body = _render_scenario_body(scenario, engine, engine_override, literals_mod, language, manifest)
        rendered_scenarios.append({
            "name": scenario.name,
            "section": scenario.section,
            "body": body,
        })

    template_name = f"{language}.jinja"
    template = env.get_template(template_name)

    return template.render(
        language=language,
        engine=engine,
        spec=spec,
        manifest=manifest,
        engine_override=engine_override,
        scenarios=rendered_scenarios,
    )


def _render_scenario_body(
    scenario: Scenario,
    engine: str,
    eo: EngineOverride,
    lit: Any,
    language: str,
    manifest: Manifest | None = None,
) -> str:
    """Render the body of a test function (the steps)."""
    lines = []
    for step in scenario.steps:
        lines.extend(_render_step(step, engine, eo, lit, language, manifest))
    return "\n".join(lines)


def _render_step(
    step: Step,
    engine: str,
    eo: EngineOverride,
    lit: Any,
    language: str,
    manifest: Manifest | None = None,
) -> list[str]:
    """Render a single step as indented source lines."""
    indent = getattr(lit, "step_indent", lambda: "    ")()
    assign = getattr(lit, "assign_op", lambda: "=")()
    decl = getattr(lit, "decl_prefix", lambda: "")()
    use_await = getattr(lit, "use_await", lambda: False)()
    func_pfx = getattr(lit, "func_prefix", lambda: "queries.")()

    if step.kind == "let":
        lines = []
        for var_name, raw_val in step.data["let"].items():
            tv = TypedValue.parse(raw_val)
            val = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)
            lines.append(f"{indent}{decl}{var_name} {assign} {val}")
        return lines

    elif step.kind == "call":
        func_name = _resolve_func_name(step.data["call"], eo, language)
        raw_args = step.data.get("args", {})
        bind = step.data.get("bind")

        param_types = _get_param_types(manifest, func_name)
        args = _render_call_args(raw_args, engine, eo, lit, param_types)
        conn = lit.conn_param()
        args_str = ", ".join([conn] + args)
        call_expr = f"{func_pfx}{func_name}({args_str})"

        # Languages with explicit error returns (Go) override via render_call_lines.
        if hasattr(lit, "render_call_lines"):
            command = _get_func_command(manifest, func_name)
            return lit.render_call_lines(call_expr, bind, command or "exec", indent)

        await_kw = "await " if use_await else ""
        call = f"{await_kw}{call_expr}"
        if bind:
            return [f"{indent}{decl}{bind} {assign} {call}"]
        return [f"{indent}{call}"]

    elif step.kind == "assert_eq":
        field_expr = step.data["field"]
        compare = step.data.get("compare")
        tv = TypedValue.parse(step.data["value"])

        if hasattr(lit, "transform_field_expr"):
            display_field = lit.transform_field_expr(field_expr)
        else:
            display_field = field_expr

        expected = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)

        if compare == "uuid_str":
            result = lit.render_uuid_compare(display_field, expected)
            return _as_lines(indent, result)

        # JSON round-tripped as a string may have different key ordering; compare parsed.
        if tv.kind == "json" and eo.type_coercions.get("json") == "json_string":
            result = lit.render_assert_json_eq(display_field, tv.value)
            return _as_lines(indent, result)

        if hasattr(lit, "render_assert_eq_typed"):
            result = lit.render_assert_eq_typed(
                display_field, expected, tv.kind, engine, eo.type_coercions
            )
        else:
            result = lit.render_assert_eq(display_field, expected)
        return _as_lines(indent, result)

    elif step.kind == "assert_null":
        expr = step.data["expr"]
        if hasattr(lit, "transform_field_expr"):
            expr = lit.transform_field_expr(expr)
        if hasattr(lit, "render_assert_null_typed"):
            result = lit.render_assert_null_typed(expr, engine, eo.type_coercions)
        else:
            result = lit.render_assert_null(expr)
        return _as_lines(indent, result)

    elif step.kind == "assert_not_null":
        expr = step.data["expr"]
        if hasattr(lit, "transform_field_expr"):
            expr = lit.transform_field_expr(expr)
        if hasattr(lit, "render_assert_not_null_typed"):
            result = lit.render_assert_not_null_typed(expr, engine, eo.type_coercions)
        else:
            result = lit.render_assert_not_null(expr)
        return _as_lines(indent, result)

    elif step.kind == "assert_len":
        tv = TypedValue.parse(step.data["value"])
        length = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)
        result = lit.render_assert_len(step.data["var"], length)
        return _as_lines(indent, result)

    return [f"{indent}# Unknown step: {step.kind}"]


def _as_lines(indent: str, result: Any) -> list[str]:
    """Convert a render result (str or list[str]) to indented source lines."""
    if isinstance(result, list):
        return [f"{indent}{line}" for line in result]
    return [f"{indent}{result}"]


def _resolve_func_name(name: str, eo: EngineOverride, language: str) -> str:
    """Apply query renames and language naming convention."""
    resolved = eo.query_renames.get(name, name)
    if language in ("python", "rust"):
        return _to_snake_case(resolved)
    elif language in ("java", "kotlin", "typescript", "javascript"):
        return _to_camel_case(resolved)
    elif language == "go":
        return _to_pascal_case(resolved)
    return resolved


def _render_call_args(
    args: dict[str, Any],
    engine: str,
    eo: EngineOverride,
    lit_mod: Any,
    param_types: dict[str, str] | None = None,
) -> list[str]:
    """Render function call arguments as a list of source strings."""
    result = []
    for arg_name, raw_val in args.items():
        tv = TypedValue.parse(raw_val)
        lang_type = (param_types or {}).get(arg_name)
        if hasattr(lit_mod, "render_typed_arg") and lang_type is not None:
            result.append(
                lit_mod.render_typed_arg(arg_name, lang_type, tv.kind, tv.value, engine, eo.type_coercions)
            )
        else:
            result.append(lit_mod.render_value(tv.kind, tv.value, engine, eo.type_coercions))
    return result


def _get_param_types(manifest: Manifest | None, func_name: str) -> dict[str, str]:
    """Return {param_name: lang_type} for a function from the manifest."""
    if manifest is None:
        return {}
    fn = manifest.get_function(func_name)
    if fn is None:
        return {}
    return {p.name: p.lang_type for p in fn.params}


def _get_func_command(manifest: Manifest | None, func_name: str) -> str | None:
    """Return the command type (exec/one/many/execrows) for a function."""
    if manifest is None:
        return None
    fn = manifest.get_function(func_name)
    return fn.command if fn else None


def _load_literals(language: str):
    """Dynamically import the literals module for a language."""
    import importlib

    return importlib.import_module(f"literals.{language}")


# ── Naming helpers ──────────────────────────────────────────────────────


def _to_snake_case(s: str) -> str:
    """PascalCase/camelCase → snake_case."""
    out = []
    for i, c in enumerate(s):
        if c.isupper() and i > 0:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


def _to_camel_case(s: str) -> str:
    """PascalCase → camelCase, snake_case → camelCase."""
    pascal = _to_pascal_case(s)
    if not pascal:
        return pascal
    return pascal[0].lower() + pascal[1:]


def _to_pascal_case(s: str) -> str:
    """snake_case → PascalCase, PascalCase stays."""
    if "_" in s:
        return "".join(w.capitalize() for w in s.split("_"))
    return s[0].upper() + s[1:] if s else s
