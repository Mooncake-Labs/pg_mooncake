#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

#include "columnstore/columnstore.hpp"
#include "duckdb_scan.hpp"

extern "C" {
PG_MODULE_MAGIC;

void _PG_init(void) {
    InitColumnstore();
    InitDuckdbScan();
}
}
