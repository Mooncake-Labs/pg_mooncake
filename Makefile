PG_VERSION ?= pg17
export PG_CONFIG := $(shell cargo pgrx info pg-config $(PG_VERSION))
MAKEFLAGS := --no-print-directory

.PHONY: help clean format package pgduckdb run test

help:
	@echo "Usage: make <COMMAND> [OPTIONS]"
	@echo ""
	@echo "Commands:"
	@echo "  run           Build and run pg_mooncake for development"
	@echo "  format        Format the codebase"
	@echo "  test          Run all tests"
	@echo "  package       Build an installation package for release"
	@echo "  install       Install pg_mooncake locally"
	@echo "  clean         Remove all build artifacts"
	@echo ""
	@echo "Options:"
	@echo "  PG_VERSION    pg14, pg15, pg16, or pg17 (default)"

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
	@if [ ! -f target/.pg_version ] || [ $$(cat target/.pg_version) != $(PG_VERSION) ]; then \
		$(MAKE) -C pg_duckdb clean;  \
		mkdir -p target; \
		echo $(PG_VERSION) > target/.pg_version; \
	fi
	@$(MAKE) -C pg_duckdb all-static-lib duckdb DUCKDB_BUILD=ReleaseStatic

run: pgduckdb
	@cargo pgrx run

test: pgduckdb
	@cargo pgrx regress --resetdb
