#pragma once

#include "duckdb/common/string.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

struct DeltaRecord {
    // Directory to store delta records.
    string path;
    // Delta options.
    string delta_options;
    // File names under [path].
    vector<string> file_names;
    // Corresponds to [file_names].
    vector<int64_t> file_sizes;
    // If it indicates add operation, corresponds to [file_names].
    vector<int8_t> is_add_files;
};

void LakeCreateTable(Oid oid, const string &path);

void LakeAddFile(Oid oid, string file_name, int64_t file_size);

void LakeDeleteFile(Oid oid, string file_name);

void LakeAbort();

void LakeCommit();

// Dump the given [delta_record] into storage.
void LakeDumpDeltaRecords(const DeltaRecord &delta_record);

} // namespace duckdb
