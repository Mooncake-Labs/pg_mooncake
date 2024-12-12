#pragma once

#include "duckdb/common/string.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

void LakeCreateTable(Oid oid, const string &path);

void LakeAddFile(Oid oid, string file_name, int64_t file_size);

void LakeDeleteFile(Oid oid, string file_name);

void LakeAbort();

void LakeCommit();

} // namespace duckdb
