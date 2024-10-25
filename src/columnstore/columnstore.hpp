#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ClientContext;
class LogicalDelete;
class LogicalInsert;
class LogicalUpdate;
class PhysicalOperator;
using Oid = unsigned int;

class Columnstore {
public:
    static void CreateTable(Oid oid);

    static void DropTable(Oid oid);

    static void TruncateTable(Oid oid);

    static unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan);
};

} // namespace duckdb
