#include "lakehouse_interface.hpp"
#include "columnstore/columnstore.hpp"
#include <cassert>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

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
#include "rust_extensions/delta.hpp"

class LakeHouseExporter {
  public:
    void ExportCreateTable(std::string const &table_name, std::string const &location, std::string const &secret_name,
                           std::vector<::std::string> const &column_names,
                           std::vector<::std::string> const &column_types) {
        try {
            CreateDeltaTable(table_name, location, secret_name, column_names, column_types);
        } catch (const std::exception &e) {
            elog(ERROR, "Error in create delta table: %s", e.what());
        }
    }

    void LogWriteOperation(Oid oid, std::string const &file_id, int64 file_size) {
        m_current_xact_state.push_back({ADD_FILE, oid, file_id, file_size});
        if (m_table_info_cache.find(oid) == m_table_info_cache.end()) {
            m_table_info_cache[oid] = TablesGet(oid).path;
        }
    }

    void ExportCommittedXact() {
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
            for (auto op : m_current_xact_state) {
                if (op.table_id == table_id) {

                    append_files.push_back(op.file_id);
                    file_sizes.push_back(op.file_size);
                }
            }
            try {
                AddFilesDeltaTable(m_table_info_cache[table_id].c_str(), "" /* secret */, append_files, file_sizes);
            } catch (const std::exception &e) {
                elog(ERROR, "Error in exporting into delta table: %s", e.what());
            }
        }
        m_current_xact_state.clear();
    }

  private:
    enum Operation { ADD_FILE, DELETE_FILE, PARTIAL_DELETE };

    struct LogEntry {
        Operation operation;
        Oid table_id;
        std::string file_id;
        int64 file_size;
    };
    std::vector<LogEntry> m_current_xact_state;
    std::unordered_map<Oid, std::string> m_table_info_cache;
};

LakeHouseExporter lake_exporter;

void LakeHouseCreateTable(Oid relid, const char *path, const char *table_format) {
    assert(strcmp(table_format, "delta") == 0);
    Relation relation = RelationIdGetRelation(relid);
    TupleDesc desc = RelationGetDescr(relation);
    std::vector<std::string> types;
    std::vector<std::string> names;
    std::string table_name = get_rel_name(relid);
    for (int col = 0; col < desc->natts; col++) {
        Form_pg_attribute attr = &desc->attrs[col];
        types.push_back(format_type_be(attr->atttypid));
        names.push_back(NameStr(attr->attname));
    }
    lake_exporter.ExportCreateTable(table_name, path, "" /*seceret*/, names, types);
    RelationClose(relation);
}

void LakeHouseLogAppendFile(Oid relid, const char *file_id, int64 file_size) {
    lake_exporter.LogWriteOperation(relid, file_id, file_size);
}

void LakeHouseCommitXact() {
    lake_exporter.ExportCommittedXact();
}