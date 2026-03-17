.PHONY: clean clean-postgresql clean-sqlite \
        generate generate-postgresql generate-sqlite \
        run run-postgresql run-sqlite

clean-postgresql:
	$(MAKE) -C postgresql clean

clean-sqlite:
	$(MAKE) -C sqlite clean

clean: clean-postgresql clean-sqlite

generate-postgresql:
	$(MAKE) -C postgresql generate

generate-sqlite:
	$(MAKE) -C sqlite generate

generate: generate-postgresql generate-sqlite

run-postgresql:
	$(MAKE) -C postgresql run

run-sqlite:
	$(MAKE) -C sqlite run

run: run-postgresql run-sqlite
