extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"
}

void InitDuckdbHooks();
void InitDuckdbScan();

extern "C" {
PG_MODULE_MAGIC;

bool allow_local_disk_table = true;

char *default_storage_bucket = NULL;

void _PG_init(void) {
    /* Define the global boolean GUC variable */
    DefineCustomBoolVariable(
        "mooncake.allow_local_disk_table",                                         /* GUC name */
        "Specifies whether creating columnstore tables on local disk is allowed.", /* Short description */
        NULL,                                                                      /* Long description */
        &allow_local_disk_table, true,                                             /* Default value (false) */
        PGC_SUSET,                                                                 /* Superuser-settable */
        0,                                                                         /* Flags */
        NULL,                                                                      /* check_hook */
        NULL,                                                                      /* assign_hook */
        NULL /* show_hook */);
    DefineCustomStringVariable("mooncake.default_storage_bucket",            /* GUC name */
                               "Default storage bucket columnstore tables.", /* Short description */
                               NULL,                                         /* Long description */
                               &default_storage_bucket,                      /* Pointer to the variable */
                               NULL,                                         /* Default value */
                               PGC_SUSET,                                    /* Superuser-settable */
                               0,                                            /* Flags */
                               NULL,                                         /* check_hook */
                               NULL,                                         /* assign_hook */
                               NULL                                          /* show_hook */
    );

    InitDuckdbHooks();
    InitDuckdbScan();
}
}
