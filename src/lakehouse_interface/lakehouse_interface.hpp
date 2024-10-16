#pragma once

extern "C" {
#include "postgres.h"
#include "postgres_ext.h"
}

void LakeHouseCreateTable(Oid oid, const char *path, const char *table_format);

void LakeHouseLogAppendFile(Oid oid, const char *file_id, int64 file_size);

void LakeHouseCommitXact();
