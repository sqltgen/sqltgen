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

    # Load the language-specific literal renderer
    literals_mod = _load_literals(language)

    # Pre-render all scenario bodies
    rendered_scenarios = []
    for scenario in spec.scenarios:
        body = _render_scenario_body(scenario, engine, engine_override, literals_mod, language)
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
) -> str:
    """Render the body of a test function (the steps)."""
    lines = []
    for step in scenario.steps:
        lines.extend(_render_step(step, engine, eo, lit, language))
    return "\n".join(lines)


def _render_step(
    step: Step,
    engine: str,
    eo: EngineOverride,
    lit: Any,
    language: str,
) -> list[str]:
    """Render a single step as indented source lines."""
    indent = "    "

    use_await = getattr(lit, "use_await", lambda: False)()
    decl = getattr(lit, "decl_prefix", lambda: "")()

    if step.kind == "let":
        lines = []
        for var_name, raw_val in step.data["let"].items():
            tv = TypedValue.parse(raw_val)
            val = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)
            lines.append(f"{indent}{decl}{var_name} = {val}")
        return lines

    elif step.kind == "call":
        func_name = _resolve_func_name(step.data["call"], eo, language)
        args = _render_call_args(step.data.get("args", {}), engine, eo, lit)
        bind = step.data.get("bind")
        args_str = ", ".join([lit.conn_param()] + args)
        await_kw = "await " if use_await else ""
        call = f"{await_kw}queries.{func_name}({args_str})"
        if bind:
            return [f"{indent}{decl}{bind} = {call}"]
        return [f"{indent}{call}"]

    elif step.kind == "assert_eq":
        field_expr = step.data["field"]
        compare = step.data.get("compare")
        tv = TypedValue.parse(step.data["value"])
        expected = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)
        if compare == "uuid_str":
            return [f"{indent}{lit.render_uuid_compare(field_expr, expected)}"]
        # JSON round-tripped as a string may have different key ordering; compare parsed.
        if tv.kind == "json" and eo.type_coercions.get("json") == "json_string":
            return [f"{indent}{lit.render_assert_json_eq(field_expr, tv.value)}"]
        if hasattr(lit, "render_assert_eq_typed"):
            line = lit.render_assert_eq_typed(
                field_expr, expected, tv.kind, engine, eo.type_coercions
            )
        else:
            line = lit.render_assert_eq(field_expr, expected)
        return [f"{indent}{line}"]

    elif step.kind == "assert_null":
        return [f"{indent}{lit.render_assert_null(step.data['expr'])}"]

    elif step.kind == "assert_not_null":
        return [f"{indent}{lit.render_assert_not_null(step.data['expr'])}"]

    elif step.kind == "assert_len":
        tv = TypedValue.parse(step.data["value"])
        length = lit.render_value(tv.kind, tv.value, engine, eo.type_coercions)
        return [f"{indent}{lit.render_assert_len(step.data['var'], length)}"]

    return [f"{indent}# Unknown step: {step.kind}"]


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
) -> list[str]:
    """Render function call arguments as a list of source strings."""
    result = []
    for _name, raw_val in args.items():
        tv = TypedValue.parse(raw_val)
        result.append(lit_mod.render_value(tv.kind, tv.value, engine, eo.type_coercions))
    return result


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
