extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

#include "duckdb_scan.hpp"
#include "lib.hpp"

extern "C" {
PG_MODULE_MAGIC;

void _PG_init(void) {
    InitDuckdbScan();
}

PG_FUNCTION_INFO_V1(add_nums);
Datum add_nums(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(add_nums_impl(arg1, arg2));
}

PG_FUNCTION_INFO_V1(sub_nums);
Datum sub_nums(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(sub_nums_impl(arg1, arg2));
}
}
