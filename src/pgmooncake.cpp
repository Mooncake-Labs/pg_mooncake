extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

void DuckdbInitHooks();

extern "C" {
PG_MODULE_MAGIC;

void DuckdbInitNode();

void _PG_init() {
    DuckdbInitHooks();
    DuckdbInitNode();
}
}
