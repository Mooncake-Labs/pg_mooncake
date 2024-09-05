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

PG_CPPFLAGS = -Isrc
PG_CXXFLAGS += -Werror

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: format

format:
	find src -name '*.c' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' | xargs clang-format -i
