#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

class ClientContext;
class LogicalDelete;
class LogicalInsert;
class LogicalUpdate;
class PhysicalOperator;

class Columnstore {
public:
    static void CreateTable(Oid oid);

    static void TruncateTable(Oid oid);

    static void VacuumTable(Oid oid);

    // Caller must ensure that the table is a columnstore table.
    // - oid: relation oid
    // - flags: PERFORM_DELETION_xxx defined in dependency.h
    static void DropTable(Oid oid, int flags);

    static void Abort();

    static void Commit();

    static void LoadSecrets(ClientContext &context);

    static unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan);
};

} // namespace duckdb
