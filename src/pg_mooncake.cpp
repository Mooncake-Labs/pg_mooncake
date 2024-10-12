extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

void InitDuckdbHooks();
void InitDuckdbScan();

extern "C" {
PG_MODULE_MAGIC;

void _PG_init(void) {
    InitDuckdbHooks();
    InitDuckdbScan();
}
}
