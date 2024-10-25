#pragma once

#include <vector>

extern "C" {
#include "postgres.h"
}

void TablesAdd(Oid oid, const char *path);

const char *TablesGet(Oid oid);

void DataFilesAdd(Oid oid, const char *file_name);

std::vector<const char *> DataFilesGet(Oid oid);
