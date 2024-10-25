export override DEBUG := $(filter debug,$(BUILD_TYPE))
CARGO_FLAGS := $(if $(DEBUG),,--release)
MAKEFLAGS := --no-print-directory

.PHONY: .BUILD all release debug clean format install uninstall \
        duckdb duckdb-fast clean-duckdb \
        delta clean-delta format-delta

.BUILD:
ifeq ($(findstring $(BUILD_TYPE),debug release),)
	@echo "Invalid BUILD_TYPE = $(BUILD_TYPE)"; exit 1
endif
	@mkdir -p build/${BUILD_TYPE}
	@rm -f build/current
	@ln -s $(BUILD_TYPE) build/current

all: duckdb-fast delta | .BUILD
	install -C Makefile.build build/$(BUILD_TYPE)/Makefile
	@$(MAKE) -C build/$(BUILD_TYPE)

release:
	@$(MAKE) BUILD_TYPE=release all

debug:
	@$(MAKE) BUILD_TYPE=debug all

clean: clean-delta
	rm -rf build

format: format-delta
	find src -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' | xargs clang-format -i

install:
	@$(MAKE) -C build/current install

uninstall:
	@$(MAKE) -C build/current uninstall

# DuckDB

duckdb: | .BUILD
	CMAKE_VARS_BUILD="-DBUILD_SHELL=0 -DBUILD_UNITTESTS=0" DISABLE_SANITIZER=1 \
	$(MAKE) -C third_party/duckdb $(BUILD_TYPE)
ifeq ($(BUILD_TYPE), debug)
	gdb-add-index third_party/duckdb/build/debug/src/libduckdb.so
endif

duckdb-fast: third_party/duckdb/build/$(BUILD_TYPE)/src/libduckdb.so | .BUILD
	install -C $< build/$(BUILD_TYPE)/libduckdb.so

clean-duckdb:
	$(MAKE) -C third_party/duckdb clean

third_party/duckdb/build/$(BUILD_TYPE)/src/libduckdb.so: | .BUILD
	@$(MAKE) duckdb

# Delta

delta: | .BUILD
	cargo build --manifest-path=rust_extensions/delta/Cargo.toml $(CARGO_FLAGS)
	@mkdir -p build/src/rust_extensions
	install -C $$(readlink -f rust_extensions/delta/target/cxxbridge/delta/src/lib.rs.h) build/src/rust_extensions/delta.hpp
	install -C rust_extensions/delta/target/$(BUILD_TYPE)/libdelta.a build/$(BUILD_TYPE)/libdelta.a

clean-delta:
	cargo clean --manifest-path=rust_extensions/delta/Cargo.toml
	rm -f rust_extensions/delta/Cargo.lock

format-delta:
	cargo fmt --manifest-path=rust_extensions/delta/Cargo.toml
