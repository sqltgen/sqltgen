# Shared paths for all e2e runtime test Makefiles.
# Include from any tests/e2e/runtime/{suite}/{lang}/{db}/Makefile.
RUNTIME_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT   := $(abspath $(RUNTIME_DIR)/../../..)
SQLTGEN     := $(REPO_ROOT)/target/debug/sqltgen
COMPOSE     := $(RUNTIME_DIR)/docker-compose.yml
