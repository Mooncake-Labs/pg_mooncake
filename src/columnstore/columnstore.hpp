#pragma once

extern "C" {
#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/relcache.h"
}

void InitColumnstore();

bool IsColumnstore(Relation table);

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
