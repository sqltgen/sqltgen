include ../../../common.mk

.PHONY: generate test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	python3 -m pytest test_runtime.py -v

clean:
	rm -rf gen __pycache__ .pytest_cache
