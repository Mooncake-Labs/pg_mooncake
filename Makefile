# ========================
# Shared Variables
# ========================
BUILD_DIR := build/$(BUILD_TYPE)
BUILD_RUST_DIR := build/src/rust_extensions
CURRENT_BUILD_DIR := build/current
DELTA_DIR := rust_extensions/delta
DELTA_HEADER := $(DELTA_DIR)/target/cxxbridge/delta/src/lib.rs.h
DELTA_LIB := $(DELTA_DIR)/target/$(BUILD_TYPE)/libdelta.a
DUCKDB_DIR := third_party/duckdb
DUCKDB_LIB := $(DUCKDB_DIR)/build/$(BUILD_TYPE)/src/libduckdb.so
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
.PHONY: .BUILD all clean clean-all clean-delta clean-duckdb debug delta \
        duckdb duckdb-fast format format-delta help install installcheck \
        release uninstall

# ========================
# Default Target: help
# ========================
help:
	@echo "Usage: make [TARGET]"
	@echo "Available targets:"
	@echo "  debug            Build in debug mode (BUILD_TYPE=debug)"
	@echo "  release          Build in release mode (BUILD_TYPE=release)"
	@echo "  clean            Remove build artifacts"
	@echo "  clean-all        Remove all build artifacts and clean everything"
	@echo "  install          Install built artifacts"
	@echo "  installcheck     Run regression tests"
	@echo "  uninstall        Uninstall built artifacts"
	@echo "  format           Format source files"

# ========================
# Build Targets
# ========================
# Exposed Build Targets
debug:
	@$(MAKE) BUILD_TYPE=debug all

release:
	@$(MAKE) BUILD_TYPE=release all

# Internal Build Targets
all: duckdb-fast delta | .BUILD
	install -C Makefile.build $(BUILD_DIR)/Makefile
	@$(MAKE) -C $(BUILD_DIR)

.BUILD: | $(BUILD_DIR)
ifeq ($(findstring $(BUILD_TYPE),debug release),)
	@echo "Invalid BUILD_TYPE = $(BUILD_TYPE)"; exit 1
endif
	@rm -f $(CURRENT_BUILD_DIR)
	@ln -s $(BUILD_TYPE) $(CURRENT_BUILD_DIR)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

# ========================
# Clean Targets
# ========================
# Exposed Clean Targets
clean:
	rm -rf build

clean-all: clean clean-duckdb clean-delta

# Internal Clean Targets
clean-duckdb:
	$(MAKE) -C $(DUCKDB_DIR) clean

clean-delta:
	cargo clean --manifest-path=$(DELTA_DIR)/Cargo.toml
	rm -f $(DELTA_DIR)/Cargo.lock

# ========================
# Install Targets
# ========================
# Exposed Install Targets
install:
	@$(MAKE) -C $(CURRENT_BUILD_DIR) install

installcheck:
	@$(MAKE) -C $(CURRENT_BUILD_DIR) installcheck

uninstall:
	@$(MAKE) -C $(CURRENT_BUILD_DIR) uninstall

# ========================
# Format Targets
# ========================
# Exposed Format Targets
format: format-delta
	find $(SRC_DIR) -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' | xargs clang-format -i

# Internal Format Targets
format-delta:
	cargo fmt --manifest-path=$(DELTA_DIR)/Cargo.toml

# ========================
# DuckDB Targets
# ========================
# Internal DuckDB Targets
duckdb-fast: $(DUCKDB_LIB)
	install -C $< $(BUILD_DIR)/libduckdb.so

duckdb: | .BUILD
	BUILD_EXTENSIONS="httpfs;json" CMAKE_VARS_BUILD="-DBUILD_SHELL=0 -DBUILD_UNITTESTS=0" DISABLE_SANITIZER=1 \
	CMAKE_BUILD_PARALLEL_LEVEL=$(or $(patsubst -j%,%,$(filter -j%,$(MAKEFLAGS))),1) \
	$(MAKE) -C $(DUCKDB_DIR) $(BUILD_TYPE)
ifeq ($(BUILD_TYPE), debug)
	gdb-add-index $(DUCKDB_LIB)
endif

$(DUCKDB_LIB): | .BUILD
	@$(MAKE) duckdb

# ========================
# Delta Targets
# ========================
# Internal Delta Targets
delta: | .BUILD $(BUILD_RUST_DIR)
	cargo build --manifest-path=$(DELTA_DIR)/Cargo.toml $(CARGO_FLAGS)
	install -C $$(readlink -f $(DELTA_HEADER)) $(BUILD_RUST_DIR)/delta.hpp
	install -C $(DELTA_LIB) $(BUILD_DIR)/libdelta.a

$(BUILD_RUST_DIR):
	@mkdir -p $@
