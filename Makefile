PG_VERSION ?= pg18
PG_CONFIG ?= $(shell cargo pgrx info pg-config $(PG_VERSION))
export PG_CONFIG
JOBS ?= 4
DUCKDB_VERSION := v2.0.0
MOONCAKE_BUILD ?= duckdb_mooncake/build/release
MOONCAKE_REPOSITORY ?= $(shell $(PG_CONFIG) --sharedir)/extension/pg_mooncake
PACKAGE_DIR ?= $(CURDIR)/target/release/pg_mooncake-$(PG_VERSION)
PGRX_FEATURES = --no-default-features --features $(PG_VERSION),bgworker
PGRX_FLAGS = $(PGRX_FEATURES) --pg-config $(PG_CONFIG)
REGRESS_DB ?= mooncake_regress
MAKEFLAGS += --no-print-directory

.PHONY: help clean duckdb_mooncake format install install-duckdb-mooncake package pg_duckdb run test

help:
	@echo "Usage: make <COMMAND> [OPTIONS]"
	@echo ""
	@echo "Commands:"
	@echo "  run           Build and run pg_mooncake for development"
	@echo "  install       Build and install pg_mooncake"
	@echo "  pg_duckdb     Build and install pg_duckdb"
	@echo "  duckdb_mooncake Build the DuckDB 2.0 Mooncake extension"
	@echo "  install-duckdb-mooncake Install it into a local extension repository"
	@echo "  package       Build an installation package for release"
	@echo "  format        Format the codebase"
	@echo "  test          Run SQL regressions against a configured test server"
	@echo "  clean         Remove build artifacts"
	@echo ""
	@echo "Options:"
	@echo "  PG_VERSION    pg14, pg15, pg16, pg17, or pg18 (default)"

clean:
	@cargo clean

duckdb_mooncake:
	cmake -S duckdb_mooncake/duckdb -B $(MOONCAKE_BUILD) -G Ninja \
		-DCMAKE_BUILD_TYPE=Release -DOVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION) \
		-DDUCKDB_EXTENSION_CONFIGS=$(CURDIR)/duckdb_mooncake/extension_config.cmake \
		-DEXTENSION_STATIC_BUILD=0 -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=OFF
	cmake --build $(MOONCAKE_BUILD) --target mooncake_loadable_extension --parallel $(JOBS)

install-duckdb-mooncake: duckdb_mooncake
	install -d "$(DESTDIR)$(MOONCAKE_REPOSITORY)/$(DUCKDB_VERSION)/$$(cat $(MOONCAKE_BUILD)/duckdb_platform_out)"
	install -m 644 $(MOONCAKE_BUILD)/extension/mooncake/mooncake.duckdb_extension \
		"$(DESTDIR)$(MOONCAKE_REPOSITORY)/$(DUCKDB_VERSION)/$$(cat $(MOONCAKE_BUILD)/duckdb_platform_out)/"

format:
	@cargo fmt
	@cargo clippy

install: install-duckdb-mooncake
	@cargo pgrx install --release $(PGRX_FLAGS)

package: duckdb_mooncake
	@cargo pgrx package --release $(PGRX_FLAGS) --out-dir "$(PACKAGE_DIR)"
	install -d "$(PACKAGE_DIR)$(MOONCAKE_REPOSITORY)/$(DUCKDB_VERSION)/$$(cat $(MOONCAKE_BUILD)/duckdb_platform_out)"
	install -m 644 $(MOONCAKE_BUILD)/extension/mooncake/mooncake.duckdb_extension \
		"$(PACKAGE_DIR)$(MOONCAKE_REPOSITORY)/$(DUCKDB_VERSION)/$$(cat $(MOONCAKE_BUILD)/duckdb_platform_out)/"

pg_duckdb:
	@$(MAKE) -C pg_duckdb install -j$(JOBS)

run: pg_duckdb install-duckdb-mooncake
	@cargo pgrx run $(PG_VERSION) $(PGRX_FEATURES)

test:
	mkdir -p target/regress
	$(shell $(PG_CONFIG) --pkglibdir)/pgxs/src/test/regress/pg_regress \
		--inputdir=tests/pg_regress --outputdir=target/regress \
		--load-extension=pg_duckdb --dbname=$(REGRESS_DB) setup sanity partitioned_table
