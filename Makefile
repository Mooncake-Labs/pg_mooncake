PG_VERSION ?= pg18
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
	@echo "  PG_VERSION    pg14, pg15, pg16, pg17, or pg18 (default)"

clean:
	@cargo clean

duckdb_mooncake:
	@$(MAKE) -C duckdb_mooncake GEN=ninja OVERRIDE_GIT_DESCRIBE=v1.4.1

format:
	@cargo fmt
	@cargo clippy

install:
	@cargo pgrx install --release

package:
	@cargo pgrx package

pg_duckdb:
	@$(MAKE) -C pg_duckdb install -j$(shell nproc)

run: pg_duckdb
	@cargo pgrx run

# cargo-pgrx 0.16 exits 0 when tests fail (it only fails in its auto-accept mode), and pg_regress leaves
# regression.diffs behind only when a test failed.
test:
	@rm -f tests/pg_regress/regression.diffs
	@cargo pgrx regress --resetdb
	@if [ -e tests/pg_regress/regression.diffs ]; then \
		cat tests/pg_regress/regression.diffs; \
		echo "make test: pg_regress tests failed (see tests/pg_regress/regression.diffs)" >&2; \
		exit 1; \
	fi
