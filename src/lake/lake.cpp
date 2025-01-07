#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "pgmooncake_guc.hpp"
#include "rust_extensions/delta.hpp"

#include <utility>

namespace duckdb {

namespace {

class LakeWriter {
public:
    LakeWriter() {
        DeltaInit();
    }

public:
    void CreateTable(Oid oid, const string &path) {
        ColumnstoreMetadata metadata(NULL /*snapshot*/);
        auto [table_name, column_names, column_types] = metadata.GetTableMetadata(oid);
        auto [type, options] = metadata.SecretsSearchDeltaOptions(path);
        DeltaCreateTable(table_name, path, type, options, column_names, column_types);
    }

    void ChangeFile(Oid oid, string file_name, int64_t file_size, bool is_add_file) {
        if (cached_table_infos.count(oid) == 0) {
            ColumnstoreMetadata metadata(NULL /*snapshot*/);
            auto [path, timeline_id] = metadata.TablesSearch(oid);
            auto [type, options] = metadata.SecretsSearchDeltaOptions(path);
            cached_table_infos[oid] = {path, std::move(timeline_id), std::move(type), std::move(options)};
        }
        auto &files = xact_state[oid];
        auto files_iter = files.find(file_name);
        if (files_iter == files.end()) {
            files.emplace(std::move(file_name), FileInfo{file_size, is_add_file});
        } else {
            D_ASSERT(files_iter->second.is_add_file && !is_add_file);
            files.erase(files_iter);
        }
    }

    void Abort() {
        xact_state.clear();
    }

    void Commit() {
        if (xact_state.empty()) {
            return;
        }
        for (auto &[oid, files] : xact_state) {
            vector<string> file_names;
            file_names.reserve(files.size());
            vector<int64_t> file_sizes;
            file_sizes.reserve(files.size());
            vector<int8_t> is_add_files;
            is_add_files.reserve(files.size());
            for (const auto &[file_name, file_info] : files) {
                file_names.emplace_back(file_name);
                file_sizes.emplace_back(file_info.file_size);
                is_add_files.emplace_back(file_info.is_add_file);
            }
            if (!file_names.empty()) {
                auto info = cached_table_infos[oid];
                if (info.timeline_id == mooncake_timeline_id) {
                    DeltaModifyFiles(info.path, info.type, info.options, file_names, file_sizes, is_add_files);
                }
            }
        }
        xact_state.clear();
    }

private:
    struct CachedTableInfoEntry {
        string path;
        string timeline_id;
        string type;
        string options;
    };
    unordered_map<Oid, CachedTableInfoEntry> cached_table_infos;

    struct FileInfo {
        int64_t file_size;
        bool is_add_file;
    };
    unordered_map<Oid, unordered_map<string, FileInfo>> xact_state;
};

LakeWriter lake_writer;

} // namespace

void LakeCreateTable(Oid oid, const string &path) {
    lake_writer.CreateTable(oid, path);
}

void LakeAddFile(Oid oid, string file_name, int64_t file_size) {
    lake_writer.ChangeFile(oid, std::move(file_name), file_size, true /*is_add_file*/);
}

void LakeDeleteFile(Oid oid, string file_name) {
    lake_writer.ChangeFile(oid, std::move(file_name), 0 /*file_size*/, false /*is_add_file*/);
}

void LakeAbort() {
    lake_writer.Abort();
}

void LakeCommit() {
    lake_writer.Commit();
}

} // namespace duckdb
