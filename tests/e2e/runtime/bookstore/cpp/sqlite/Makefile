include ../../../common.mk

.PHONY: generate test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	docker compose up --build --abort-on-container-exit test
	@ret=$$?; docker compose rm -f test; exit $$ret

clean:
	rm -rf gen
