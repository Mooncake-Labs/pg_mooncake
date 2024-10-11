#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "utils/rel.h"
}

#include "columnstore/columnstore_table.hpp"

namespace duckdb {

TableStorageInfo ColumnstoreTableCatalogEntry::GetStorageInfo(ClientContext &context) {
    // HACK: force update_is_del_and_insert
    TableStorageInfo result;
    // result.cardinality = GetTotalRows();
    IndexInfo index_info;
    for (column_t i = 0; i < RelationGetNumberOfAttributes(rel); i++) {
        index_info.column_set.insert(i);
    }
    result.index_info.push_back(std::move(index_info));
    return result;
}

} // namespace duckdb
