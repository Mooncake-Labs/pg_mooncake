#pragma once

extern "C" {
#include "postgres.h"

#include "utils/relcache.h"
}

bool IsColumnstoreTable(Relation rel);

bool IsColumnstoreTable(Oid oid);
