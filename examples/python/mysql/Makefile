SQLTGEN    := ../../../target/debug/sqltgen
MYSQL      := ../../common/mysql
MIGRATIONS := ../../common/mysql/migrations
MYSQLHOST  := 127.0.0.1
MYSQLPORT  := 3307
MYSQLUSER  := sqltgen
MYSQLPASS  := sqltgen
VENV       := .venv
PYTHON     := $(VENV)/bin/python

.PHONY: clean generate build db-up db-down run run-shared

$(VENV):
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -q -r requirements.txt

clean:
	rm -rf gen $(VENV)

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate $(VENV)

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

# run-shared: container already running (used by run-all).
run-shared: build
	MIGRATIONS_DIR=$(MIGRATIONS) $(PYTHON) main.py

# run: standalone — starts its own container, runs, tears down.
run: build
	$(MAKE) -C $(MYSQL) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) $(PYTHON) main.py; ret=$$?; $(MAKE) -C $(MYSQL) db-down || true; exit $$ret
