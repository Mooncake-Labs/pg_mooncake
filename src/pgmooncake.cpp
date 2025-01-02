#include "duckdb/common/file_system.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"
}

void MooncakeInitGUC();
void DuckdbInitHooks();

bool mooncake_allow_local_tables = true;
char *mooncake_default_bucket = strdup("");
bool mooncake_enable_local_cache = true;
const char *mooncake_timeline_id = "main";

extern "C" {
PG_MODULE_MAGIC;

void DuckdbInitNode();

void _PG_init() {
    MooncakeInitGUC();
    DuckdbInitHooks();
    DuckdbInitNode();
    pgduckdb::RegisterDuckdbXactCallback();

    const char *neon_timeline_id =
        GetConfigOption("neon.timeline_id", true /*missing_ok*/, false /*restrict_privileged*/);
    if (neon_timeline_id) {
        mooncake_timeline_id = neon_timeline_id;
    }

    auto local_fs = duckdb::FileSystem::CreateLocal();
    local_fs->CreateDirectory("mooncake_local_cache");
    local_fs->CreateDirectory("mooncake_local_tables");
}
}
