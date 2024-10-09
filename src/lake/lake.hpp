#pragma once

extern "C" {
#include "postgres.h"
}

void LakeCreateTable(Oid oid, const char *path);

void LakeAddFile(Oid oid, const char *file_id, int64 file_size);

void LakeCommit();
