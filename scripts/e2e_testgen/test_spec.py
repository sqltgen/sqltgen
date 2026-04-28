"""Parse test_spec.yaml into a data model."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class TypedValue:
    """An abstract typed value from the spec.

    Exactly one of these is set:
      - str, int, float, bool (primitives)
      - null (True when explicitly null)
      - json (dict or list)
      - uuid ("random" or a literal UUID string)
      - datetime, date, time (ISO strings)
      - var (reference to a named variable)
    """

    kind: str  # "str", "int", "json", "uuid", "datetime", "date", "time", "null", "var", etc.
    value: Any  # The raw value

    @staticmethod
    def parse(raw: Any) -> TypedValue:
        """Parse a typed value from YAML.

        A typed value is a dict with exactly one key (the type) and one value.
        YAML converts `null` to Python `None`, so `{null: true}` becomes `{None: True}`.
        """
        if isinstance(raw, dict) and len(raw) == 1:
            kind, value = next(iter(raw.items()))
            # YAML converts the key `null` to Python None
            if kind is None:
                return TypedValue(kind="null", value=value)
            return TypedValue(kind=str(kind), value=value)
        # Bare scalars: infer type
        if isinstance(raw, int):
            return TypedValue(kind="int", value=raw)
        if isinstance(raw, float):
            return TypedValue(kind="float", value=raw)
        if isinstance(raw, str):
            return TypedValue(kind="str", value=raw)
        if isinstance(raw, bool):
            return TypedValue(kind="bool", value=raw)
        raise ValueError(f"Expected a typed value dict (e.g. {{str: 'x'}}), got: {raw!r}")


@dataclass
class Step:
    """A single step in a test scenario."""

    kind: str  # "call", "let", "assert_eq", "assert_null", "assert_not_null", "assert_len"
    data: dict[str, Any]

    @staticmethod
    def parse(raw: dict[str, Any]) -> Step:
        """Parse a step from YAML."""
        if "call" in raw:
            return Step(kind="call", data=raw)
        if "assert_eq" in raw:
            return Step(kind="assert_eq", data=raw["assert_eq"])
        if "assert_null" in raw:
            return Step(kind="assert_null", data={"expr": raw["assert_null"]})
        if "assert_not_null" in raw:
            return Step(kind="assert_not_null", data={"expr": raw["assert_not_null"]})
        if "assert_len" in raw:
            return Step(kind="assert_len", data=raw["assert_len"])
        raise ValueError(f"Unknown step type: {raw!r}")


@dataclass
class Scenario:
    """A test scenario with a name and a list of steps."""

    name: str
    section: str
    steps: list[Step]

    @staticmethod
    def parse(raw: dict[str, Any]) -> Scenario:
        return Scenario(
            name=raw["name"],
            section=raw.get("section", ""),
            steps=[Step.parse(s) for s in raw["steps"]],
        )


@dataclass
class EngineOverride:
    """Engine-specific overrides for query names and type coercions."""

    query_renames: dict[str, str] = field(default_factory=dict)
    type_coercions: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict[str, Any] | None) -> EngineOverride:
        if raw is None:
            return EngineOverride()
        return EngineOverride(
            query_renames=raw.get("query_renames", {}),
            type_coercions=raw.get("type_coercions", {}),
        )


@dataclass
class TestSpec:
    """Parsed test specification."""

    fixture: str
    scenarios: list[Scenario]
    engine_overrides: dict[str, EngineOverride]
    languages: list[str] | None  # None = all languages
    engines: list[str] | None  # None = all engines
    sqltgen_overrides: dict[str, Any]  # e.g. {"type_overrides": {"json": "gson"}}
    variant: str  # e.g. "gson", "" for default/baseline

    @staticmethod
    def load(path: str | Path) -> TestSpec:
        """Load and parse a test_spec.yaml file."""
        with open(path) as f:
            raw = yaml.safe_load(f)
        return TestSpec(
            fixture=raw["fixture"],
            scenarios=[Scenario.parse(s) for s in raw["scenarios"]],
            engine_overrides={
                engine: EngineOverride.parse(overrides)
                for engine, overrides in raw.get("engine_overrides", {}).items()
            },
            languages=raw.get("languages"),
            engines=raw.get("engines"),
            sqltgen_overrides=raw.get("sqltgen_overrides", {}),
            variant=raw.get("variant", ""),
        )

    def get_engine_override(self, engine: str) -> EngineOverride:
        """Return overrides for an engine, or empty defaults."""
        return self.engine_overrides.get(engine, EngineOverride())
