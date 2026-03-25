"""Parse manifest.json emitted by sqltgen."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ManifestField:
    """A field in a model or a parameter in a function."""

    name: str
    lang_type: str
    sql_type: str
    nullable: bool


@dataclass
class ManifestModel:
    """A generated model (struct, dataclass, interface, record)."""

    name: str
    fields: list[ManifestField]


@dataclass
class ManifestFunction:
    """A generated query function."""

    name: str
    command: str
    params: list[ManifestField]
    returns: str | None


@dataclass
class Manifest:
    """Parsed sqltgen manifest."""

    language: str
    engine: str
    package: str
    models: list[ManifestModel]
    functions: list[ManifestFunction]

    @staticmethod
    def load(path: str | Path) -> Manifest:
        """Load and parse a manifest.json file."""
        with open(path) as f:
            raw = json.load(f)
        return Manifest(
            language=raw["language"],
            engine=raw["engine"],
            package=raw["package"],
            models=[
                ManifestModel(
                    name=m["name"],
                    fields=[ManifestField(**f) for f in m["fields"]],
                )
                for m in raw["models"]
            ],
            functions=[
                ManifestFunction(
                    name=fn["name"],
                    command=fn["command"],
                    params=[ManifestField(**p) for p in fn["params"]],
                    returns=fn.get("returns"),
                )
                for fn in raw["functions"]
            ],
        )

    def get_function(self, name: str) -> ManifestFunction | None:
        """Find a function by name."""
        for fn in self.functions:
            if fn.name == name:
                return fn
        return None

    def get_model(self, name: str) -> ManifestModel | None:
        """Find a model by name."""
        for m in self.models:
            if m.name == name:
                return m
        return None

    def get_param_type(self, func_name: str, param_name: str) -> str | None:
        """Look up the language type for a specific parameter."""
        fn = self.get_function(func_name)
        if fn is None:
            return None
        for p in fn.params:
            if p.name == param_name:
                return p.lang_type
        return None
