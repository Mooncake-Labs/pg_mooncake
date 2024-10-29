extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"
}

void InitDuckdbHooks();
void InitDuckdbScan();

extern "C" {
PG_MODULE_MAGIC;

bool allow_local_disk_table = false;

char *default_storage_bucket = NULL;

bool enable_local_cache = true;

void _PG_init(void) {
    /* Define the global boolean GUC variable */
    DefineCustomBoolVariable(
        "pg_mooncake.allow_local_disk_table",                                      /* GUC name */
        "Specifies whether creating columnstore tables on local disk is allowed.", /* Short description */
        NULL,                                                                      /* Long description */
        &allow_local_disk_table, false,                                             /* Default value (false) */
        PGC_SUSET,                                                                 /* Superuser-settable */
        0,                                                                         /* Flags */
        NULL,                                                                      /* check_hook */
        NULL,                                                                      /* assign_hook */
        NULL /* show_hook */);
    DefineCustomStringVariable("pg_mooncake.default_storage_bucket",         /* GUC name */
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
    DefineCustomBoolVariable("pg_mooncake.enable_local_cache",                           /* GUC name */
                             "Specifies whether cache remote data files on local disk.", /* Short description */
                             NULL,                                                       /* Long description */
                             &allow_local_disk_table, true,                              /* Default value (false) */
                             PGC_SUSET,                                                  /* Superuser-settable */
                             0,                                                          /* Flags */
                             NULL,                                                       /* check_hook */
                             NULL,                                                       /* assign_hook */
                             NULL /* show_hook */);

    InitDuckdbHooks();
    InitDuckdbScan();
}
}
