# ========================
# Shared Variables
# ========================
BUILD_TYPE ?= debug
BUILD_DIR := build/$(BUILD_TYPE)
BUILD_SRC_RUST := $(BUILD_DIR)/src/rust_extensions
CURRENT_BUILD := build/current
DUCKDB_DIR := third_party/duckdb
DELTA_DIR := rust_extensions/delta
DUCKDB_BUILD := $(DUCKDB_DIR)/build/$(BUILD_TYPE)/src/libduckdb.so
DELTA_LIB := $(DELTA_DIR)/target/$(BUILD_TYPE)/libdelta.a
DELTA_HEADER := $(DELTA_DIR)/target/cxxbridge/delta/src/lib.rs.h
SRC_DIR := src

# ========================
# Flags
# ========================
export override DEBUG := $(filter debug,$(BUILD_TYPE))
CARGO_FLAGS := $(if $(DEBUG),,--release)
MAKEFLAGS := --no-print-directory

# ========================
# Phony Targets
# ========================
.PHONY: .BUILD all release debug clean format install installcheck uninstall \
        duckdb duckdb-fast clean-duckdb \
        delta clean-delta format-delta help

# ========================
# Default Target: Help
# ========================
help:
	@echo "Usage: make [TARGET] [BUILD_TYPE=debug|release]"
	@echo ""
	@echo "Default BUILD_TYPE is 'debug'."
	@echo ""
	@echo "Available targets:"
	@echo "  all              Build DuckDB and Delta extension"
	@echo "  release          Build in release mode (BUILD_TYPE=release)"
	@echo "  debug            Build in debug mode (BUILD_TYPE=debug)"
	@echo "  clean            Remove build artifacts"
	@echo "  format           Format source files"
	@echo "  install          Install built artifacts"
	@echo "  installcheck     Run regression tests"
	@echo "  uninstall        Uninstall built artifacts"
	@echo "  duckdb           Build DuckDB library"
	@echo "  duckdb-fast      Install prebuilt DuckDB binary"
	@echo "  clean-duckdb     Clean DuckDB build artifacts"
	@echo "  delta            Build Delta extension"
	@echo "  clean-delta      Clean Delta extension artifacts"
	@echo "  format-delta     Format Delta extension code"

# ========================
# Build Setup
# ========================
.BUILD:
ifeq ($(findstring $(BUILD_TYPE),debug release),)
	@echo "Invalid BUILD_TYPE = $(BUILD_TYPE)"; exit 1
endif
	@mkdir -p $(BUILD_DIR)
	@rm -f $(CURRENT_BUILD)
	@ln -s $(BUILD_TYPE) $(CURRENT_BUILD)

all: duckdb-fast delta | .BUILD
	install -C Makefile.build $(BUILD_DIR)/Makefile
	@$(MAKE) -C $(BUILD_DIR)

release:
	@$(MAKE) BUILD_TYPE=release all

debug:
	@$(MAKE) BUILD_TYPE=debug all

clean: clean-delta
	rm -rf build

format: format-delta
	find $(SRC_DIR) -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' | xargs clang-format -i

install:
	@$(MAKE) -C $(CURRENT_BUILD) install

installcheck:
	@$(MAKE) -C $(CURRENT_BUILD) installcheck

uninstall:
	@$(MAKE) -C $(CURRENT_BUILD) uninstall

# ========================
# DuckDB Targets
# ========================
duckdb: | .BUILD
	BUILD_EXTENSIONS="httpfs;json" CMAKE_VARS_BUILD="-DBUILD_SHELL=0 -DBUILD_UNITTESTS=0" DISABLE_SANITIZER=1 \
	CMAKE_BUILD_PARALLEL_LEVEL=$(or $(patsubst -j%,%,$(filter -j%,$(MAKEFLAGS))),1) \
	$(MAKE) -C $(DUCKDB_DIR) $(BUILD_TYPE)
ifeq ($(BUILD_TYPE), debug)
	gdb-add-index $(DUCKDB_BUILD)
endif

duckdb-fast: $(DUCKDB_BUILD) | .BUILD
	install -C $< $(BUILD_DIR)/libduckdb.so

clean-duckdb:
	$(MAKE) -C $(DUCKDB_DIR) clean

$(DUCKDB_BUILD): | .BUILD
	@$(MAKE) duckdb

# ========================
# Delta Extension Targets
# ========================
delta: | .BUILD
	cargo build --manifest-path=$(DELTA_DIR)/Cargo.toml $(CARGO_FLAGS)
	@mkdir -p $(BUILD_SRC_RUST)
	install -C $(shell readlink -f $(DELTA_HEADER)) $(BUILD_SRC_RUST)/delta.hpp
	install -C $(DELTA_LIB) $(BUILD_DIR)/libdelta.a

clean-delta:
	cargo clean --manifest-path=$(DELTA_DIR)/Cargo.toml
	rm -f $(DELTA_DIR)/Cargo.lock

format-delta:
	cargo fmt --manifest-path=$(DELTA_DIR)/Cargo.toml
