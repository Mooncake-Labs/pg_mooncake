PG_VERSION ?= pg18
# The PostgreSQL to build for. A PG_CONFIG from the command line or the environment wins; otherwise it is the one
# `cargo pgrx init` registered for PG_VERSION, looked up once here. Targets that need it check it (check-pg-config), so
# targets that don't, such as duckdb_mooncake, work without cargo-pgrx.
ifndef PG_CONFIG
PG_CONFIG := $(shell cargo pgrx info pg-config $(PG_VERSION) 2>/dev/null)
endif
# A relative path would break in pg_duckdb's make, which runs in another directory; a bare name is looked up on PATH.
ifneq (,$(findstring /,$(PG_CONFIG)))
override PG_CONFIG := $(abspath $(PG_CONFIG))
endif
export PG_CONFIG
MAKEFLAGS += --no-print-directory

.PHONY: help check-pg-config check-rust-postgres clean duckdb_mooncake format install package pg_duckdb run test

help:
	@echo "Usage: make <COMMAND> [OPTIONS]"
	@echo ""
	@echo "Commands:"
	@echo "  help                 Show this help"
	@echo "  run                  Build and install pg_duckdb, then build pg_mooncake and run it in a pgrx-managed Postgres"
	@echo "  install              Build and install pg_mooncake"
	@echo "  pg_duckdb            Build and install pg_duckdb"
	@echo "  duckdb_mooncake      Build the duckdb_mooncake DuckDB extension"
	@echo "  package              Build an installation package for release"
	@echo "  test                 Run the pg_regress tests (tests/pg_regress) on the pgrx-managed Postgres"
	@echo "  format               Format the code (cargo fmt) and run cargo clippy"
	@echo "  clean                Remove build artifacts"
	@echo "  check-pg-config      Check that PG_CONFIG is a pg_config of PG_VERSION"
	@echo "  check-rust-postgres  Check that rust-postgres is pinned to moonlink's revision"
	@echo ""
	@echo "Options:"
	@echo "  PG_VERSION           pg14, pg15, pg16, pg17, or pg18 (default)"
	@echo "  PG_CONFIG            pg_config of the PostgreSQL to build for (default: the one cargo pgrx init"
	@echo "                       registered for PG_VERSION); must be the same major as PG_VERSION"

check-pg-config:
	@if [ -z "$(PG_CONFIG)" ]; then \
		echo "PG_CONFIG is empty: run cargo pgrx init --$(PG_VERSION) <path to pg_config>, or pass PG_CONFIG=<path to pg_config>" >&2; \
		exit 1; \
	fi
	@major=$$("$(PG_CONFIG)" --version 2>/dev/null | sed -n 's/^PostgreSQL \([0-9][0-9]*\).*/\1/p'); \
	if [ -z "$$major" ]; then echo "PG_CONFIG=$(PG_CONFIG) is not a working pg_config" >&2; exit 1; fi; \
	if [ "pg$$major" != "$(PG_VERSION)" ]; then \
		echo "PG_CONFIG=$(PG_CONFIG) is PostgreSQL $$major, but PG_VERSION=$(PG_VERSION); pass PG_VERSION=pg$$major" >&2; \
		exit 1; \
	fi

# pg_mooncake must pin rust-postgres to the revision moonlink uses, or Cargo builds a second copy of it.
check-rust-postgres:
	@ours=$$(sed -n 's/.*rust-postgres\.git", rev = "\([0-9a-f]*\)".*/\1/p' Cargo.toml | sort -u); \
	theirs=$$(grep -i 'rust-postgres' moonlink/Cargo.toml | sed -n 's/.*rev = "\([0-9a-f]*\)".*/\1/p' | sort -u); \
	if [ -z "$$ours" ] || [ -z "$$theirs" ] || [ "$$(echo "$$ours" | wc -l)" != 1 ] || [ "$$ours" != "$$theirs" ]; then \
		echo "rust-postgres revisions differ: Cargo.toml has [$$ours], moonlink/Cargo.toml has [$$theirs]" >&2; \
		exit 1; \
	fi; \
	echo "rust-postgres pinned to $$ours, as in moonlink"

clean:
	@cargo clean

# DuckDB version duckdb_mooncake is built for; it must equal pg_duckdb's (DUCKDB_VERSION in pg_duckdb/Makefile), which
# CI checks.
DUCKDB_VERSION := v1.4.1

duckdb_mooncake:
	@$(MAKE) -C duckdb_mooncake GEN=ninja OVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION)

format:
	@cargo fmt
	@cargo clippy

install: check-pg-config
	@cargo pgrx install --release --pg-config "$(PG_CONFIG)"

package: check-pg-config
	@cargo pgrx package --pg-config "$(PG_CONFIG)"

# PG_CONFIG is passed explicitly: a command-line PG_CONFIG would otherwise reach this make through MAKEFLAGS unchanged.
pg_duckdb: check-pg-config
	@$(MAKE) -C pg_duckdb install -j$(shell nproc) PG_CONFIG="$(PG_CONFIG)"

run: pg_duckdb
	@cargo pgrx run $(PG_VERSION)

# cargo-pgrx 0.16 exits 0 when tests fail (it only fails in its auto-accept mode), and pg_regress leaves
# regression.diffs behind only when a test failed.
test: check-pg-config
	@rm -f tests/pg_regress/regression.diffs
	@cargo pgrx regress --resetdb
	@if [ -e tests/pg_regress/regression.diffs ]; then \
		cat tests/pg_regress/regression.diffs; \
		echo "make test: pg_regress tests failed (see tests/pg_regress/regression.diffs)" >&2; \
		exit 1; \
	fi
