#pragma once

#include "duckdb.hpp"

#include "duckdb/parser/tableref.hpp"

extern "C" {
#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/relcache.h"
}

void InitColumnstore();

bool IsColumnstore(Oid relid);

void ColumnstoreInsert(Relation rel, TupleTableSlot **slots, int nslots);

duckdb::unique_ptr<duckdb::TableRef> ColumnstoreReplacementScan(const duckdb::string &table_name);
