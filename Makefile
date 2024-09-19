MODULE_big = pg_mooncake
EXTENSION = pg_mooncake
DATA = sql/pg_mooncake--0.0.1.sql

SRCS := $(shell find src -name '*.c' -o -name '*.cpp')
OBJS := $(addsuffix .o, $(basename $(SRCS)))

BUILD_TYPE ?= debug
ifeq ($(BUILD_TYPE), debug)
    PG_CXXFLAGS = -ggdb3 -O0
else ifeq ($(BUILD_TYPE), release)
    PG_CXXFLAGS =
else
    $(error Invalid BUILD_TYPE)
endif

LIBDUCKDB_SO := third_party/duckdb/build/$(BUILD_TYPE)/src/libduckdb.so
PG_CPPFLAGS = -Isrc \
              -Ithird_party/duckdb/src/include
PG_CXXFLAGS += -Werror -Wno-sign-compare
SHLIB_PREREQS = $(PG_LIB)/libduckdb.so
SHLIB_LINK = -L$(PG_LIB) -Wl,-rpath,$(PG_LIB) -lduckdb

PG_CONFIG ?= pg_config
PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: format clean-duckdb duckdb install-duckdb

format:
	find src -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' | xargs clang-format -i

clean-duckdb:
	$(MAKE) -C third_party/duckdb clean

duckdb: $(LIBDUCKDB_SO)

install-duckdb: $(PG_LIB)/libduckdb.so

$(LIBDUCKDB_SO):
	$(MAKE) -C third_party/duckdb $(BUILD_TYPE) DISABLE_SANITIZER=1 ENABLE_UBSAN=0 BUILD_UNITTESTS=OFF
	if [ "$(BUILD_TYPE)" = "debug" ]; then gdb-add-index $(LIBDUCKDB_SO); fi

$(PG_LIB)/libduckdb.so: $(LIBDUCKDB_SO)
	$(install_bin) -m 755 $(LIBDUCKDB_SO) $(PG_LIB)

install: install-duckdb
