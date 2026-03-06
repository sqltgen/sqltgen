SQLTGEN := ../../../target/debug/sqltgen
MYSQL   := ../../common/mysql
VENV    := .venv
PYTHON  := $(VENV)/bin/python

.PHONY: generate build db-up db-down run

$(VENV):
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -q -r requirements.txt

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate $(VENV)

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

run: build
	$(MAKE) db-up
	$(PYTHON) main.py; ret=$$?; $(MAKE) db-down; exit $$ret
