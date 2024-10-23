#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ClientContext;
class LogicalDelete;
class LogicalInsert;
class LogicalUpdate;
class PhysicalOperator;
typedef unsigned int Oid;

class Columnstore {
public:
    static void CreateTable(ClientContext &context, Oid oid);

    static void DropTable(Oid oid);

    static void TruncateTable(Oid oid);

    static string GetTableInfo(Oid oid);

    static string GetSecretForPath(const string &path);

    static unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan);

    static unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan);
};

} // namespace duckdb
