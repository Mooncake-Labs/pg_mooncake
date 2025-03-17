MAKEFLAGS := --no-print-directory

.PHONY: help clean format package pgduckdb run test

help:
	@echo "Usage: make <COMMAND>"
	@echo ""
	@echo "Commands:"
	@echo "  run        Build and run pg_mooncake for development"
	@echo "  format     Format the codebase"
	@echo "  test       Run all tests"
	@echo "  package    Build an installation package for release"
	@echo "  install    Install pg_mooncake locally"
	@echo "  clean      Remove all build artifacts"

clean:
	@$(MAKE) -C pg_duckdb clean-all
	@cargo clean

format:
	@$(MAKE) -C pg_duckdb format-all
	@cargo fmt
	@cargo clippy

install: pgduckdb
	@cargo pgrx install --release

package: pgduckdb
	@cargo pgrx package

pgduckdb:
	@$(MAKE) -C pg_duckdb all-static-lib duckdb DUCKDB_BUILD=ReleaseStatic DUCKDB_GEN=make

run: pgduckdb
	@cargo pgrx run

test: pgduckdb
	@cargo pgrx regress --resetdb
