#pragma once

#include "pgduckdb/pg/transactions.hpp"

namespace duckdb {

void MooncakeXactCallback(XactEvent event);
void MooncakeSubXactCallback(SubXactEvent event);

} // namespace duckdb