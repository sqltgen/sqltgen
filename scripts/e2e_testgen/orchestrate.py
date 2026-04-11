#!/usr/bin/env python3
"""E2E test generator orchestrator.

Usage:
    orchestrate.py generate [--fixture NAME] [--lang LANG] [--engine ENGINE]
    orchestrate.py run      [--fixture NAME] [--lang LANG] [--engine ENGINE]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from codegen import render_test
from manifest import Manifest
from test_spec import TestSpec

# Paths relative to the sqltgen repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "e2e" / "fixtures"
RUNTIME_DIR = REPO_ROOT / "tests" / "e2e" / "runtime"
_runtime_dir = RUNTIME_DIR  # Mutable; overridden by --runtime-dir CLI flag.

# Known dimensions for test generation.
KNOWN_LANGUAGES = ["python", "go", "typescript", "javascript", "rust", "java", "kotlin"]
KNOWN_ENGINES = ["postgresql", "sqlite", "mysql"]

# Non-default variants available per language.
# A variant names the one thing deviating from the language default
# (e.g. "gson" for Gson JSON library instead of Jackson).
KNOWN_VARIANTS: dict[str, list[str]] = {
    "java": ["gson"],
    "kotlin": ["gson"],
}

# Output test file names per language.
TEST_FILE_NAMES = {
    "python": "test_runtime_gen.py",
    "go": "runtime_gen_test.go",
    "typescript": "runtime_gen.test.ts",
    "javascript": "runtime_gen.test.js",
    "rust": "tests/runtime_gen.rs",
    "java": "src/test/java/com/example/db/RuntimeGenTest.java",
    "kotlin": "src/test/kotlin/com/example/db/RuntimeGenTest.kt",
}


@dataclass
class Combo:
    """A (fixture, language, engine, variant) combination to generate/run.

    variant names the one thing deviating from the language default (e.g. "gson").
    Empty string means all defaults.
    """

    fixture: str
    language: str
    engine: str
    variant: str = ""

    @property
    def engine_dir(self) -> str:
        """Directory name: engine + variant suffix when non-default."""
        return f"{self.engine}-{self.variant}" if self.variant else self.engine

    @property
    def fixture_dir(self) -> Path:
        return FIXTURES_DIR / self.fixture

    @property
    def runtime_dir(self) -> Path:
        return _runtime_dir / self.fixture / self.language / self.engine_dir

    @property
    def spec_path(self) -> Path:
        return self.fixture_dir / "test_spec.yaml"

    @property
    def sqltgen_json(self) -> Path:
        return self.runtime_dir / "sqltgen.json"

    @property
    def manifest_path(self) -> Path:
        return self.runtime_dir / "gen" / "manifest.json"

    @property
    def output_test_file(self) -> Path:
        return self.runtime_dir / TEST_FILE_NAMES[self.language]


def discover_combos(
    fixture: str | None = None,
    lang: str | None = None,
    engine: str | None = None,
    variant: str | None = None,
) -> list[Combo]:
    """Find all valid combos matching filters.

    For each language, iterates the default variant (empty) plus any
    language-specific variants from KNOWN_VARIANTS. Only combos whose
    sqltgen.json exists on disk are included.
    """
    combos = []
    fixtures = [fixture] if fixture else [d.name for d in FIXTURES_DIR.iterdir() if d.is_dir()]

    for fix in fixtures:
        spec_path = FIXTURES_DIR / fix / "test_spec.yaml"
        if not spec_path.exists():
            continue

        languages = [lang] if lang else KNOWN_LANGUAGES
        engines = [engine] if engine else KNOWN_ENGINES

        for l in languages:
            variants = [variant] if variant is not None else [""] + KNOWN_VARIANTS.get(l, [])
            for e in engines:
                for v in variants:
                    combo = Combo(fixture=fix, language=l, engine=e, variant=v)
                    if combo.sqltgen_json.exists():
                        combos.append(combo)
    return combos


def ensure_manifest(combo: Combo, sqltgen_binary: str = "cargo") -> Path:
    """Run sqltgen generate to produce the manifest, adding manifest key if needed.

    Returns the path to the manifest.json file.
    """
    config_path = combo.sqltgen_json
    with open(config_path) as f:
        config = json.load(f)

    # Find the language key in gen
    lang_key = combo.language
    if lang_key not in config.get("gen", {}):
        available = list(config.get("gen", {}).keys())
        raise KeyError(
            f"Language '{combo.language}' not found in gen config of {config_path}. "
            f"Available keys: {available}"
        )

    gen_config = config["gen"][lang_key]

    # Add manifest key if not present
    manifest_rel = gen_config.get("out", "gen") + "/manifest.json"
    if "manifest" not in gen_config:
        gen_config["manifest"] = manifest_rel

    # Write modified config to a temp file in the same directory
    tmp_config = config_path.parent / ".sqltgen_testgen.json"
    with open(tmp_config, "w") as f:
        json.dump(config, f, indent=2)

    try:
        # Run sqltgen generate
        cmd = [sqltgen_binary, "run", "--", "generate", "--config", str(tmp_config)]
        if sqltgen_binary != "cargo":
            cmd = [sqltgen_binary, "generate", "--config", str(tmp_config)]

        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"sqltgen generate failed for {combo.language}/{combo.engine_dir}:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            sys.exit(1)
    finally:
        tmp_config.unlink(missing_ok=True)

    return Path(config_path.parent / manifest_rel)


def generate(combo: Combo, sqltgen_binary: str = "cargo") -> Path:
    """Generate a test file for a combo. Returns the output path."""
    # 1. Load spec
    spec = TestSpec.load(combo.spec_path)

    # 2. Ensure manifest exists
    manifest_path = ensure_manifest(combo, sqltgen_binary)
    manifest = Manifest.load(manifest_path)

    # 3. Render test
    engine_override = spec.get_engine_override(combo.engine)
    test_code = render_test(combo.language, combo.engine, spec, manifest, engine_override, combo.variant)

    # 4. Write test file
    output = combo.output_test_file
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(test_code)
    print(f"Generated: {output.relative_to(REPO_ROOT)}")

    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="E2E test generator")
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd_name in ("generate", "run"):
        p = sub.add_parser(cmd_name)
        p.add_argument("--fixture", help="Fixture name (e.g. type_overrides)")
        p.add_argument("--lang", help="Target language (e.g. python)")
        p.add_argument("--engine", help="Database engine (e.g. postgresql)")
        p.add_argument("--variant", help="Non-default variant (e.g. gson)")
        p.add_argument("--sqltgen", default="cargo", help="sqltgen binary or 'cargo' (default)")
        p.add_argument("--runtime-dir", help="Override runtime directory (default: tests/e2e/runtime)")

    args = parser.parse_args()

    global _runtime_dir
    if args.runtime_dir:
        _runtime_dir = Path(args.runtime_dir).resolve()

    combos = discover_combos(args.fixture, args.lang, args.engine, args.variant)

    if not combos:
        print("No matching combos found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(combos)} combo(s):")
    for c in combos:
        print(f"  {c.fixture}/{c.language}/{c.engine_dir}")

    skipped = []

    if args.command == "generate":
        for combo in combos:
            try:
                generate(combo, args.sqltgen)
            except ModuleNotFoundError as e:
                print(f"  Skipped {combo.language}/{combo.engine_dir}: {e}", file=sys.stderr)
                skipped.append(combo)

    elif args.command == "run":
        for combo in combos:
            try:
                generate(combo, args.sqltgen)
            except ModuleNotFoundError as e:
                print(f"  Skipped {combo.language}/{combo.engine_dir}: {e}", file=sys.stderr)
                skipped.append(combo)
        # TODO: Add language-specific test runners
        print("\nTest running not yet implemented. Generated files are ready for manual testing.")

    if skipped:
        print(f"\n{len(skipped)} combo(s) skipped (renderer not yet implemented):"
              f" {', '.join(f'{c.language}/{c.engine_dir}' for c in skipped)}", file=sys.stderr)


if __name__ == "__main__":
    main()
