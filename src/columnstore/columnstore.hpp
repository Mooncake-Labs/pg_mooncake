#pragma once

#include "duckdb.hpp"

#include "duckdb/parser/tableref.hpp"

extern "C" {
#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/relcache.h"
}

void InitColumnstore();

bool IsColumnstore(Oid oid);

struct ColumnstoreOptions {
    const char *path = "";
};

void TablesAdd(Oid oid, const ColumnstoreOptions &options);

ColumnstoreOptions TablesGet(Oid oid);

void DataFilesAdd(Oid oid, const char *file_name);

std::vector<const char *> DataFilesGet(Oid oid);

void ColumnstoreCreateTable(Oid oid, const ColumnstoreOptions &options);

void ColumnstoreInsert(Relation table, TupleTableSlot **slots, int nslots);

void ColumnstoreFinalize();

duckdb::unique_ptr<duckdb::TableRef> ColumnstoreReplacementScan(Oid oid, const duckdb::string &table_name);
