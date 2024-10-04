#pragma once

#include "duckdb.hpp"

#include "duckdb/parser/tableref.hpp"

extern "C" {
#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/relcache.h"
}

constexpr const char *x_columnstore_access_method = "columnstore";

void InitColumnstore();

void DataFilesAdd(Oid relid, const char *file_name);

duckdb::vector<duckdb::Value> DataFilesGet(Oid relid);

bool IsColumnstore(Oid relid);

void ColumnstoreInsert(Relation rel, TupleTableSlot **slots, int nslots);

duckdb::unique_ptr<duckdb::TableRef> ColumnstoreReplacementScan(Oid relid, const duckdb::string &table_name);

void TableInfoAdd(Oid relid, const char *storagePath, const char *lakeFormat);

void TableInfoGet(Oid relid, const char **storage_path /*out*/, const char **lake_format /*out*/);