PG_VERSION ?= pg17
export PG_CONFIG := $(shell cargo pgrx info pg-config $(PG_VERSION))
MAKEFLAGS += --no-print-directory

.PHONY: help clean duckdb_mooncake format install package pg_duckdb run test

help:
	@echo "Usage: make <COMMAND> [OPTIONS]"
	@echo ""
	@echo "Commands:"
	@echo "  run           Build and run pg_mooncake for development"
	@echo "  install       Build and install pg_mooncake"
	@echo "  pg_duckdb     Build and install pg_duckdb"
	@echo "  package       Build an installation package for release"
	@echo "  format        Format the codebase"
	@echo "  test          Run all tests"
	@echo "  clean         Remove build artifacts"
	@echo ""
	@echo "Options:"
	@echo "  PG_VERSION    pg14, pg15, pg16, or pg17 (default)"

clean:
	@cargo clean

duckdb_mooncake:
	@$(MAKE) -C duckdb_mooncake GEN=ninja

format:
	@cargo fmt
	@cargo clippy

install:
	@cargo pgrx install --release

package:
	@cargo pgrx package

pg_duckdb:
	@$(MAKE) -C pg_duckdb install -j$(nproc)

run: pg_duckdb
	@cargo pgrx run

test:
	@cargo pgrx regress --resetdb
