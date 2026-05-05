# Shared snapshot-gate logic for e2e runtime test combos.
#
# Each per-combo Makefile must:
#   - Set OUTPUT_DIR (the sqltgen output directory, e.g. src/db, gen)
#   - Provide a `generate` target that regenerates OUTPUT_DIR
#   - Provide a `runtime-test` target that compiles and runs the tests
#
# This file provides the `test` target. It runs `generate`, then diffs
# OUTPUT_DIR against the git index. Behavior:
#
#   FORCE_RUNTIME=1            → always run runtime (escape hatch)
#   CI=1, dirty diff           → fail-fast (don't run runtime)
#   CI=1, clean diff           → run runtime
#   Local (no CI), dirty diff  → run runtime
#   Local (no CI), clean diff  → skip runtime (snapshot is trusted)
#
# OUTPUT_DIR is inspected via `git status --porcelain` so untracked,
# modified, and deleted files all register as drift.

.PHONY: test

test: generate
	@dirty=$$(git status --porcelain -- $(OUTPUT_DIR) 2>/dev/null); \
	if [ -n "$$FORCE_RUNTIME" ]; then \
		echo "── runtime forced (FORCE_RUNTIME) ──"; \
		$(MAKE) --no-print-directory runtime-test; \
	elif [ -n "$$dirty" ]; then \
		if [ -n "$$CI" ]; then \
			echo ""; \
			echo "──────────────────────────────────────────────────────────────"; \
			echo "  Snapshot drift in $(OUTPUT_DIR)"; \
			echo "  ($(CURDIR))"; \
			echo "  Regenerate locally, verify with runtime, commit, push."; \
			echo "──────────────────────────────────────────────────────────────"; \
			git --no-pager status --short -- $(OUTPUT_DIR); \
			echo ""; \
			git --no-pager diff -- $(OUTPUT_DIR) | head -120; \
			exit 1; \
		else \
			echo "── snapshot drift, running runtime ──"; \
			$(MAKE) --no-print-directory runtime-test; \
		fi; \
	else \
		if [ -n "$$CI" ]; then \
			$(MAKE) --no-print-directory runtime-test; \
		else \
			echo "── snapshot clean, runtime skipped ──"; \
		fi; \
	fi
