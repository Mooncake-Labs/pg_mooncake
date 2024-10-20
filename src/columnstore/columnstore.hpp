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

int64_t DataFilesAdd(Oid oid, const char *file_name);

std::vector<const char *> DataFilesGet(Oid oid);

void CacheAdd(Oid oid, int64_t file_id);

void ColumnstoreCreateTable(Oid oid, const ColumnstoreOptions &options);

void ColumnstoreInsert(Relation table, TupleTableSlot **slots, int nslots);

void ColumnstoreFinalize();

const char *SecretGetForPath(const char *path);