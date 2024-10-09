#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_class.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
}

#include "columnstore/columnstore.hpp"
#include "rust_extensions/delta.hpp"

class LakeWriter {
public:
    enum Operation { ADD_FILE, DELETE_FILE, PARTIAL_DELETE };

    LakeWriter() {
        DeltaInit();
    }
    void CreateTable(std::string const &table_name, std::string const &path,
                     const std::vector<std::string> &column_names, const std::vector<std::string> &column_types) {
        try {
            DeltaCreateTable(table_name, path, duckdb::Columnstore::GetSecretForPath(path), column_names, column_types);
        } catch (const std::exception &e) {
            elog(ERROR, "Error in create delta table: %s", e.what());
        }
    }

    void ChangeFile(Oid oid, std::string const &file_name, int64 file_size, Operation op) {
        m_current_xact_state.push_back({op, oid, file_name, file_size});
        if (m_table_info_cache.find(oid) == m_table_info_cache.end()) {
            m_table_info_cache[oid] = {duckdb::Columnstore::GetTableInfo(oid),
                                       duckdb::Columnstore::GetSecretForPath(duckdb::Columnstore::GetTableInfo(oid))};
        }
    }

    void Commit() {
        if (m_current_xact_state.empty()) {
            return;
        }
        std::unordered_set<Oid> tables_in_xact;
        for (auto op : m_current_xact_state) {
            tables_in_xact.insert(op.table_id);
        }
        for (auto table_id : tables_in_xact) {
            std::vector<std::string> append_files;
            std::vector<int64> file_sizes;
            std::vector<int8> is_add_files;
            for (auto op : m_current_xact_state) {
                if (op.table_id == table_id) {
                    append_files.push_back(op.file_name);
                    file_sizes.push_back(op.file_size);
                    is_add_files.push_back(op.operation == ADD_FILE);
                }
            }
            try {
                DeltaModifyFiles(m_table_info_cache[table_id].table_path.c_str(),
                                 m_table_info_cache[table_id].secret.c_str(), append_files, file_sizes, is_add_files);
            } catch (const std::exception &e) {
                elog(ERROR, "Error in exporting into delta table: %s", e.what());
            }
        }
        m_current_xact_state.clear();
    }

private:
    struct LogEntry {
        Operation operation;
        Oid table_id;
        std::string file_name;
        int64 file_size;
    };
    std::vector<LogEntry> m_current_xact_state;

    struct TableInfoCacheEntry {
        std::string table_path;
        std::string secret;
    };
    std::unordered_map<Oid, TableInfoCacheEntry> m_table_info_cache;
};

LakeWriter lake_writer;

void LakeCreateTable(Oid oid, const char *path) {
    Relation relation = RelationIdGetRelation(oid);
    TupleDesc desc = RelationGetDescr(relation);
    std::vector<std::string> types;
    std::vector<std::string> names;
    std::string table_name = RelationGetRelationName(relation);
    for (int col = 0; col < desc->natts; col++) {
        Form_pg_attribute attr = &desc->attrs[col];
        types.push_back(format_type_be(attr->atttypid));
        names.push_back(NameStr(attr->attname));
    }
    lake_writer.CreateTable(table_name, path, names, types);
    RelationClose(relation);
}

void LakeAddFile(Oid oid, const char *file_name, int64 file_size) {
    lake_writer.ChangeFile(oid, file_name, file_size, LakeWriter::ADD_FILE);
}

void LakeDeleteFile(Oid oid, const char *file_name) {
    lake_writer.ChangeFile(oid, file_name, 0, LakeWriter::DELETE_FILE);
}

void LakeCommit() {
    lake_writer.Commit();
}
