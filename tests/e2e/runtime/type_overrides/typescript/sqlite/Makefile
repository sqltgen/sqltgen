include ../../../common.mk

.PHONY: generate test clean install

install:
	npm install --silent

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	@log=$$(mktemp); npm test > $$log 2>&1 || { cat $$log; rm -f $$log; exit 1; }; rm -f $$log

clean:
	rm -rf gen node_modules
