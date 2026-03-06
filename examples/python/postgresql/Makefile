SQLTGEN := ../../../target/debug/sqltgen
PG      := ../../common/postgresql
VENV    := .venv
PYTHON  := $(VENV)/bin/python

.PHONY: clean generate build db-up db-down run

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
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

run: build
	$(MAKE) db-up
	$(PYTHON) main.py; ret=$$?; $(MAKE) db-down; exit $$ret
