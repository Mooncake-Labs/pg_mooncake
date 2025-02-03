#include "duckdb/common/file_system.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"
}

void MooncakeInitGUC();
void DuckdbInitHooks();

const char *x_mooncake_local_cache = "mooncake_local_cache/";

bool mooncake_allow_local_tables = true;
char *mooncake_default_bucket = strdup("");
bool mooncake_enable_local_write_cache = true;
bool mooncake_enable_local_read_cache = false;
int mooncake_local_read_cache_block_size = 1ULL << 18; // 256KiB
bool mooncake_enable_memory_metadata_cache = false;
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
        mooncake_allow_local_tables = false;
        mooncake_timeline_id = neon_timeline_id;
    }

    auto local_fs = duckdb::FileSystem::CreateLocal();
    local_fs->CreateDirectory("mooncake_local_cache");
    local_fs->CreateDirectory("mooncake_local_tables");
}
}
