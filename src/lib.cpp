#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
}

int add_nums_impl(int arg1, int arg2) {
    duckdb::DuckDB db;
    duckdb::Connection con(db);
    auto result = con.Query("SELECT 'Hello, DuckDB!'");
    elog(NOTICE, "%s", result->ToString().c_str());

    return arg1 + arg2;
}

int sub_nums_impl(int arg1, int arg2) {
    return arg1 - arg2;
}
