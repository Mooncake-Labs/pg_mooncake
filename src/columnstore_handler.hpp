#pragma once

#include "pgduckdb/pg/declarations.hpp"

bool IsColumnstoreTable(Relation rel);

bool IsColumnstoreTable(Oid oid);

void CreateOrSetColumnstoreDataDir(const char *path);
