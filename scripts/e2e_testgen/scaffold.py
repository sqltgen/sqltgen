#!/usr/bin/env python3
"""Scaffold a runtime e2e test project for a fixture × language × engine combo.

Usage:
    scaffold.py --fixture bookstore --lang rust --engine postgresql
    scaffold.py --fixture bookstore --all-langs --engine postgresql
    scaffold.py --fixture bookstore --lang rust --all-engines

Creates the project boilerplate (Cargo.toml, go.mod, package.json, pom.xml,
Makefile, sqltgen.json, etc.) under tests/e2e/runtime-new/<fixture>/<lang>/<engine>/.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "e2e" / "fixtures"
RUNTIME_DIR = REPO_ROOT / "tests" / "e2e" / "runtime-new"

ALL_LANGUAGES = ["rust", "go", "python", "typescript", "javascript", "java", "kotlin"]
ALL_ENGINES = ["postgresql", "sqlite", "mysql"]

# ── sqltgen.json ──────────────────────────────────────────────────────────────

# Output directory and package per language.
LANG_OUTPUT = {
    "rust":       {"out": "src/db", "package": "db"},
    "go":         {"out": "gen",    "package": "gen"},
    "python":     {"out": "gen",    "package": "gen"},
    "typescript": {"out": "gen",    "package": ""},
    "javascript": {"out": "gen",    "package": ""},
    "java":       {"out": "src/main/java/com/example/db", "package": "com.example.db"},
    "kotlin":     {"out": "src/main/kotlin/com/example/db", "package": "com.example.db"},
}

# Map language names to sqltgen config keys.
LANG_CONFIG_KEY = {
    "javascript": "javascript",
    "typescript": "typescript",
    "rust": "rust",
    "go": "go",
    "python": "python",
    "java": "java",
    "kotlin": "kotlin",
}


def make_sqltgen_json(fixture: str, lang: str, engine: str, dest: Path,
                      type_overrides: dict[str, str] | None = None) -> None:
    """Write sqltgen.json for a combo."""
    fixture_dir = FIXTURES_DIR / fixture / engine
    schema_rel = os.path.relpath(fixture_dir / "schema.sql", dest)
    queries_rel = os.path.relpath(fixture_dir / "queries.sql", dest)

    gen_config = dict(LANG_OUTPUT[lang])
    if type_overrides:
        gen_config["type_overrides"] = type_overrides

    config = {
        "version": "1",
        "engine": engine,
        "schema": schema_rel,
        "queries": queries_rel,
        "gen": {LANG_CONFIG_KEY[lang]: gen_config},
    }

    (dest / "sqltgen.json").write_text(json.dumps(config, indent=4) + "\n")

# ── Makefile ──────────────────────────────────────────────────────────────────

MAKEFILE_RUST = textwrap.dedent("""\
    REPO_ROOT := $(shell git rev-parse --show-toplevel)
    SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

    .PHONY: generate test clean

    generate:
    \trm -rf src/db
    \t$(SQLTGEN) generate --config sqltgen.json

    test: generate
    \tcargo test

    clean:
    \tcargo clean
    \trm -rf src/db
""")

MAKEFILE_GO = textwrap.dedent("""\
    REPO_ROOT := $(shell git rev-parse --show-toplevel)
    SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

    .PHONY: generate test clean

    generate:
    \trm -rf gen
    \t$(SQLTGEN) generate --config sqltgen.json

    test: generate
    \tgo mod tidy
    \tgo test -v -count=1 ./...

    clean:
    \trm -rf gen
""")

MAKEFILE_PYTHON = textwrap.dedent("""\
    REPO_ROOT := $(shell git rev-parse --show-toplevel)
    SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

    .PHONY: generate test clean

    generate:
    \trm -rf gen
    \t$(SQLTGEN) generate --config sqltgen.json

    test: generate
    \tpython -m pytest -xvs

    clean:
    \trm -rf gen __pycache__ .pytest_cache
""")

MAKEFILE_TS = textwrap.dedent("""\
    REPO_ROOT := $(shell git rev-parse --show-toplevel)
    SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

    .PHONY: install generate test clean

    install:
    \tnpm install

    generate:
    \trm -rf gen
    \t$(SQLTGEN) generate --config sqltgen.json

    test: install generate
    \tnpm test

    clean:
    \trm -rf gen node_modules
""")

MAKEFILE_JAVA = textwrap.dedent("""\
    REPO_ROOT := $(shell git rev-parse --show-toplevel)
    SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

    .PHONY: generate test clean

    generate:
    \trm -rf src/main/java/com/example/db
    \t$(SQLTGEN) generate --config sqltgen.json

    test: generate
    \tmvn -q test

    clean:
    \tmvn -q clean
    \trm -rf src/main/java/com/example/db
""")

MAKEFILE_KOTLIN = MAKEFILE_JAVA  # Same structure.

MAKEFILES = {
    "rust": MAKEFILE_RUST,
    "go": MAKEFILE_GO,
    "python": MAKEFILE_PYTHON,
    "typescript": MAKEFILE_TS,
    "javascript": MAKEFILE_TS,
    "java": MAKEFILE_JAVA,
    "kotlin": MAKEFILE_KOTLIN,
}

# ── Language-specific project files ───────────────────────────────────────────

# sqlx features per engine for Rust.
RUST_SQLX_FEATURES = {
    "postgresql": '["runtime-tokio", "postgres", "json", "time", "rust_decimal", "uuid"]',
    "sqlite":     '["runtime-tokio", "sqlite", "time"]',
    "mysql":      '["runtime-tokio", "mysql", "rust_decimal", "time"]',
}

RUST_EXTRA_DEPS = {
    "postgresql": 'rust_decimal = "1"\ntime = "0.3"\nuuid = { version = "1", features = ["v4"] }',
    "sqlite":     'time = "0.3"\nuuid = { version = "1", features = ["v4"] }',
    "mysql":      'rust_decimal = "1"\ntime = "0.3"\nuuid = { version = "1", features = ["v4"] }',
}

def write_rust(fixture: str, engine: str, dest: Path) -> None:
    crate_name = f"e2e-{fixture}-rust-{engine}"
    features = RUST_SQLX_FEATURES[engine]
    extra = RUST_EXTRA_DEPS[engine]
    (dest / "Cargo.toml").write_text(textwrap.dedent(f"""\
        [package]
        name = "{crate_name}"
        version = "0.1.0"
        edition = "2021"
        publish = false

        [dependencies]
        serde_json = "1"
        sqlx = {{ version = "0.8", features = {features} }}
        {extra}
        tokio = {{ version = "1", features = ["macros", "rt-multi-thread"] }}
    """))
    src = dest / "src"
    src.mkdir(exist_ok=True)
    (src / "lib.rs").write_text("pub mod db;\n")
    (dest / ".gitignore").write_text("src/db\ntarget/\n")


# Go driver modules per engine.
GO_DRIVERS = {
    "postgresql": ("github.com/jackc/pgx/v5", "v5.7.4"),
    "sqlite":     ("modernc.org/sqlite", "latest"),
    "mysql":      ("github.com/go-sql-driver/mysql", "v1.9.1"),
}

def write_go(fixture: str, engine: str, dest: Path) -> None:
    mod_name = f"e2e-{fixture.replace('_', '-')}-go-{engine}"
    # go.mod is only written if it doesn't exist. go mod tidy (at test time)
    # updates the go directive and deps, so we don't overwrite it.
    if (dest / "go.mod").exists():
        return
    driver_mod, driver_ver = GO_DRIVERS[engine]
    (dest / "go.mod").write_text(textwrap.dedent(f"""\
        module {mod_name}

        go 1.23
    """))
    # We'll let `go mod tidy` resolve the full dependency tree after generation.


def write_python(fixture: str, engine: str, dest: Path) -> None:
    deps = {
        "postgresql": "psycopg>=3.0\npytest>=7.0\n",
        "sqlite":     "pytest>=7.0\n",
        "mysql":      "mysql-connector-python>=8.0\npytest>=7.0\n",
    }
    (dest / "requirements.txt").write_text(deps[engine])


# TS/JS driver deps per engine.
TS_DEPS = {
    "postgresql": {"pg": "^8.13.3"},
    "sqlite":     {"better-sqlite3": "^11.7.0"},
    "mysql":      {"mysql2": "^3.9.0"},
}
TS_DEV_DEPS = {
    "postgresql": {"@types/pg": "^8.11.10"},
    "sqlite":     {"@types/better-sqlite3": "^7.6.12"},
    "mysql":      {},
}

def write_typescript(fixture: str, engine: str, dest: Path) -> None:
    pkg_name = f"e2e-{fixture}-typescript-{engine}"
    pkg = {
        "name": pkg_name,
        "version": "0.0.0",
        "private": True,
        "scripts": {"test": "tsx --test runtime_gen.test.ts"},
        "dependencies": TS_DEPS[engine],
        "devDependencies": {
            **TS_DEV_DEPS[engine],
            "@types/node": "^22.0.0",
            "tsx": "^4.19.2",
            "typescript": "^5.7.3",
        },
    }
    (dest / "package.json").write_text(json.dumps(pkg, indent=2) + "\n")
    (dest / "tsconfig.json").write_text(json.dumps({
        "compilerOptions": {
            "target": "ES2022",
            "module": "CommonJS",
            "strict": True,
            "esModuleInterop": True,
            "skipLibCheck": True,
        }
    }, indent=2) + "\n")


def write_javascript(fixture: str, engine: str, dest: Path) -> None:
    pkg_name = f"e2e-{fixture}-javascript-{engine}"
    pkg = {
        "name": pkg_name,
        "version": "0.0.0",
        "private": True,
        "type": "module",
        "scripts": {"test": "node --test runtime_gen.test.js"},
        "dependencies": TS_DEPS[engine],
    }
    (dest / "package.json").write_text(json.dumps(pkg, indent=2) + "\n")


# Java/Kotlin JDBC driver deps per engine.
JDBC_DRIVERS = {
    "postgresql": ("org.postgresql", "postgresql", "42.7.5"),
    "sqlite":     ("org.xerial", "sqlite-jdbc", "3.49.1.0"),
    "mysql":      ("com.mysql", "mysql-connector-j", "8.3.0"),
}

def _pom_xml(fixture: str, lang: str, engine: str, extra_deps: str = "") -> str:
    group_id, artifact_id, version = JDBC_DRIVERS[engine]
    project_name = f"e2e-{fixture}-{lang}-{engine}"
    return textwrap.dedent(f"""\
        <project xmlns="http://maven.apache.org/POM/4.0.0"
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                                     http://maven.apache.org/xsd/maven-4.0.0.xsd">
          <modelVersion>4.0.0</modelVersion>

          <groupId>com.example</groupId>
          <artifactId>{project_name}</artifactId>
          <version>1.0-SNAPSHOT</version>

          <properties>
            <maven.compiler.source>21</maven.compiler.source>
            <maven.compiler.target>21</maven.compiler.target>
            <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
          </properties>

          <dependencies>
            <dependency>
              <groupId>{group_id}</groupId>
              <artifactId>{artifact_id}</artifactId>
              <version>{version}</version>
            </dependency>
            <dependency>
              <groupId>com.fasterxml.jackson.core</groupId>
              <artifactId>jackson-databind</artifactId>
              <version>2.18.3</version>
            </dependency>
        {extra_deps}
            <dependency>
              <groupId>org.junit.jupiter</groupId>
              <artifactId>junit-jupiter</artifactId>
              <version>5.11.4</version>
              <scope>test</scope>
            </dependency>
          </dependencies>

          <build>
            <plugins>
              <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
              </plugin>
            </plugins>
          </build>
        </project>
    """)


def write_java(fixture: str, engine: str, dest: Path) -> None:
    (dest / "pom.xml").write_text(_pom_xml(fixture, "java", engine))


def write_kotlin(fixture: str, engine: str, dest: Path) -> None:
    kotlin_plugin = textwrap.dedent("""\
              <plugin>
                <groupId>org.jetbrains.kotlin</groupId>
                <artifactId>kotlin-maven-plugin</artifactId>
                <version>2.1.0</version>
                <executions>
                  <execution>
                    <id>compile</id>
                    <goals><goal>compile</goal></goals>
                    <configuration>
                      <sourceDirs><sourceDir>src/main/kotlin</sourceDir></sourceDirs>
                    </configuration>
                  </execution>
                  <execution>
                    <id>test-compile</id>
                    <goals><goal>test-compile</goal></goals>
                    <configuration>
                      <sourceDirs><sourceDir>src/test/kotlin</sourceDir></sourceDirs>
                    </configuration>
                  </execution>
                </executions>
              </plugin>""")
    # Insert kotlin plugin and stdlib dep.
    pom = _pom_xml(fixture, "kotlin", engine, extra_deps=textwrap.dedent("""\
            <dependency>
              <groupId>org.jetbrains.kotlin</groupId>
              <artifactId>kotlin-stdlib</artifactId>
              <version>2.1.0</version>
            </dependency>"""))
    # Add kotlin plugin before surefire closing.
    pom = pom.replace("          </plugins>", kotlin_plugin + "\n          </plugins>")
    (dest / "pom.xml").write_text(pom)


WRITERS = {
    "rust": write_rust,
    "go": write_go,
    "python": write_python,
    "typescript": write_typescript,
    "javascript": write_javascript,
    "java": write_java,
    "kotlin": write_kotlin,
}

# ── Lock file generation ─────────────────────────────────────────────────────

def resolve_deps(lang: str, dest: Path) -> None:
    """Run the language's dependency resolver to produce lock files.

    Skips resolution if the lock file already exists (deps rarely change;
    delete the lock file to force re-resolution).
    """
    import subprocess

    if lang == "rust":
        if (dest / "Cargo.lock").exists():
            return
        subprocess.run(["cargo", "generate-lockfile"], cwd=dest, capture_output=True)
    elif lang == "go":
        # go mod tidy runs at test time (after generated code exists)
        # so we skip lock file generation during scaffolding.
        pass
    elif lang in ("typescript", "javascript"):
        if (dest / "package-lock.json").exists():
            return
        subprocess.run(["npm", "install", "--package-lock-only"], cwd=dest, capture_output=True)
    # Java/Kotlin: Maven has no lock file mechanism — skip.


# ── Main ──────────────────────────────────────────────────────────────────────

def scaffold(fixture: str, lang: str, engine: str,
             type_overrides: dict[str, str] | None = None) -> Path:
    """Create project boilerplate for one combo. Returns the directory path."""
    dest = RUNTIME_DIR / fixture / lang / engine
    dest.mkdir(parents=True, exist_ok=True)

    # sqltgen.json
    make_sqltgen_json(fixture, lang, engine, dest, type_overrides)

    # Makefile
    (dest / "Makefile").write_text(MAKEFILES[lang])

    # Language-specific files
    WRITERS[lang](fixture, engine, dest)

    # Generate lock files
    resolve_deps(lang, dest)

    print(f"  created: {dest.relative_to(REPO_ROOT)}")
    return dest


# Fixtures that need per-language sqltgen.json configs (type overrides).
# These must be scaffolded individually with --fixture, not via --all.
# Fixtures excluded from --all scaffolding.
# type_overrides: needs per-language sqltgen.json configs (type override split).
# schema_qualified: Go backend has unused import bug; will be added after fix.
FIXTURES_REQUIRING_CUSTOM_CONFIG = {"type_overrides", "schema_qualified"}

# Known-broken combos (fixture, lang, engine).
EXCLUDED_COMBOS: set[tuple[str | None, str, str]] = set()


def discover_fixtures() -> list[str]:
    """Find all fixtures that have a test_spec.yaml and work with uniform config."""
    return sorted(
        d.name for d in FIXTURES_DIR.iterdir()
        if d.is_dir()
        and (d / "test_spec.yaml").exists()
        and d.name not in FIXTURES_REQUIRING_CUSTOM_CONFIG
    )


def _is_excluded(fixture: str, lang: str, engine: str) -> bool:
    """Check if a combo is in the exclusion list."""
    return (fixture, lang, engine) in EXCLUDED_COMBOS or (None, lang, engine) in EXCLUDED_COMBOS


def scaffold_all() -> list[Path]:
    """Scaffold every valid fixture × language × engine combo."""
    results = []
    for fixture in discover_fixtures():
        for lang in ALL_LANGUAGES:
            for engine in ALL_ENGINES:
                fixture_dir = FIXTURES_DIR / fixture / engine
                if not fixture_dir.exists():
                    continue
                if _is_excluded(fixture, lang, engine):
                    continue
                results.append(scaffold(fixture, lang, engine))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Scaffold e2e runtime test projects")
    parser.add_argument("--all", action="store_true", help="Scaffold all fixtures × languages × engines")
    parser.add_argument("--fixture", help="Fixture name")
    parser.add_argument("--lang", help="Language (or --all-langs)")
    parser.add_argument("--all-langs", action="store_true", help="All languages")
    parser.add_argument("--engine", help="Engine (or --all-engines)")
    parser.add_argument("--all-engines", action="store_true", help="All engines")

    args = parser.parse_args()

    if args.all:
        scaffold_all()
        return

    if not args.fixture:
        parser.error("Specify --fixture or --all")

    langs = ALL_LANGUAGES if args.all_langs else ([args.lang] if args.lang else [])
    engines = ALL_ENGINES if args.all_engines else ([args.engine] if args.engine else [])

    if not langs:
        parser.error("Specify --lang or --all-langs")
    if not engines:
        parser.error("Specify --engine or --all-engines")

    for lang in langs:
        for engine in engines:
            fixture_dir = FIXTURES_DIR / args.fixture / engine
            if not fixture_dir.exists():
                print(f"  skip (no fixture): {args.fixture}/{engine}")
                continue
            scaffold(args.fixture, lang, engine)


if __name__ == "__main__":
    main()
