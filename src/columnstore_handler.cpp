#include "columnstore/columnstore.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"

#include "access/tableam.h"
#include "fmgr.h"
#include "utils/syscache.h"
}

struct ColumnstoreScanDescData {
    TableScanDescData rs_base;
};
using ColumnstoreScanDesc = ColumnstoreScanDescData *;

const TupleTableSlotOps *columnstore_slot_callbacks(Relation rel) {
    return &TTSOpsMinimalTuple;
}

TableScanDesc columnstore_scan_begin(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
                                     ParallelTableScanDesc pscan, uint32 flags) {
    ColumnstoreScanDesc scan = static_cast<ColumnstoreScanDesc>(palloc(sizeof(ColumnstoreScanDescData)));
    scan->rs_base.rs_rd = rel;
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_flags = flags;
    scan->rs_base.rs_parallel = pscan;
    return reinterpret_cast<TableScanDesc>(scan);
}

void columnstore_scan_end(TableScanDesc scan) {
    ColumnstoreScanDesc cscan = reinterpret_cast<ColumnstoreScanDesc>(scan);
    pfree(cscan);
}

void columnstore_scan_rescan(TableScanDesc scan, struct ScanKeyData *key, bool set_params, bool allow_strat,
                             bool allow_sync, bool allow_pagemode) {
    elog(ERROR, "columnstore_scan_rescan not implemented");
}

bool columnstore_scan_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot) {
    elog(ERROR, "columnstore_scan_getnextslot not implemented");
}

Size columnstore_parallelscan_estimate(Relation rel) {
    elog(ERROR, "columnstore_parallelscan_estimate not implemented");
}

Size columnstore_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan) {
    elog(ERROR, "columnstore_parallelscan_initialize not implemented");
}

void columnstore_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan) {
    elog(ERROR, "columnstore_parallelscan_reinitialize not implemented");
}

struct IndexFetchTableData *columnstore_index_fetch_begin(Relation rel) {
    elog(ERROR, "columnstore_index_fetch_begin not implemented");
}

void columnstore_index_fetch_reset(struct IndexFetchTableData *data) {
    elog(ERROR, "columnstore_index_fetch_reset not implemented");
}

void columnstore_index_fetch_end(struct IndexFetchTableData *data) {
    elog(ERROR, "columnstore_index_fetch_end not implemented");
}

bool columnstore_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
                                   TupleTableSlot *slot, bool *call_again, bool *all_dead) {
    elog(ERROR, "columnstore_index_fetch_tuple not implemented");
}

bool columnstore_tuple_fetch_row_version(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot) {
    elog(ERROR, "columnstore_tuple_fetch_row_version not implemented");
}

bool columnstore_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
    elog(ERROR, "columnstore_tuple_tid_valid not implemented");
}

void columnstore_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid) {
    elog(ERROR, "columnstore_tuple_get_latest_tid not implemented");
}

bool columnstore_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot) {
    elog(ERROR, "columnstore_tuple_satisfies_snapshot not implemented");
}

TransactionId columnstore_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate) {
    elog(ERROR, "columnstore_index_delete_tuples not implemented");
}

void columnstore_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
                              struct BulkInsertStateData *bistate) {
    columnstore_pg_insert(rel, &slot, 1);
}

void columnstore_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
                                          struct BulkInsertStateData *bistate, uint32 specToken) {
    elog(ERROR, "columnstore_tuple_insert_speculative not implemented");
}

void columnstore_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded) {
    elog(ERROR, "columnstore_tuple_complete_speculative not implemented");
}

void columnstore_multi_insert(Relation rel, TupleTableSlot **slots, int nslots, CommandId cid, int options,
                              struct BulkInsertStateData *bistate) {
    columnstore_pg_insert(rel, slots, nslots);
    // elog(ERROR, "columnstore_multi_insert not implemented");
}

TM_Result columnstore_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                                   bool wait, TM_FailureData *tmfd, bool changingPart) {
    elog(ERROR, "columnstore_tuple_delete not implemented");
}

#if PG_VERSION_NUM >= 160000
TM_Result columnstore_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
                                   Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
                                   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes) {
#else
TM_Result columnstore_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
                                   Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
                                   LockTupleMode *lockmode, bool *update_indexes) {
#endif
    elog(ERROR, "columnstore_tuple_update not implemented");
}

TM_Result columnstore_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid,
                                 LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd) {
    elog(ERROR, "columnstore_tuple_lock not implemented");
}

#if PG_VERSION_NUM >= 160000
void columnstore_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator, char persistence,
                                              TransactionId *freezeXid, MultiXactId *minmulti) {
#else
void columnstore_relation_set_new_filenode(Relation rel, const RelFileNode *newrnode, char persistence,
                                           TransactionId *freezeXid, MultiXactId *minmulti) {
#endif
    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(rel->rd_id));
    if (!HeapTupleIsValid(tp)) {
        TupleDesc desc = RelationGetDescr(rel);
        for (int i = 0; i < desc->natts; i++) {
            Form_pg_attribute attr = &desc->attrs[i];
            auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
            if (duck_type.id() == duckdb::LogicalTypeId::USER) {
                elog(ERROR, "column \"%s\" has unsupported type", NameStr(attr->attname));
            }
            if (attr->attgenerated) {
                elog(ERROR, "unsupported generated column \"%s\"", NameStr(attr->attname));
            }
        }

        duckdb::Columnstore::CreateTable(rel->rd_id);
    } else {
        ReleaseSysCache(tp);
        duckdb::Columnstore::TruncateTable(rel->rd_id);
    }
}

void columnstore_relation_nontransactional_truncate(Relation rel) {
    duckdb::Columnstore::TruncateTable(rel->rd_id);
}

#if PG_VERSION_NUM >= 160000
void columnstore_relation_copy_data(Relation rel, const RelFileLocator *newrlocator) {
#else
void columnstore_relation_copy_data(Relation rel, const RelFileNode *newrnode) {
#endif
    elog(ERROR, "columnstore_relation_copy_data not implemented");
}

void columnstore_relation_copy_for_cluster(Relation OldTable, Relation NewTable, Relation OldIndex, bool use_sort,
                                           TransactionId OldestXmin, TransactionId *xid_cutoff,
                                           MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed,
                                           double *tups_recently_dead) {
    elog(ERROR, "columnstore_relation_copy_for_cluster not implemented");
}

void columnstore_relation_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy) {
    elog(ERROR, "columnstore_relation_vacuum not implemented");
}

#if PG_VERSION_NUM >= 170000
bool columnstore_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream) {
#else
bool columnstore_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno, BufferAccessStrategy bstrategy) {
#endif
    return false;
}

bool columnstore_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows,
                                         double *deadrows, TupleTableSlot *slot) {
    elog(ERROR, "columnstore_scan_analyze_next_tuple not implemented");
}

double columnstore_index_build_range_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
                                          bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
                                          BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
                                          TableScanDesc scan) {
    elog(ERROR, "columnstore_index_build_range_scan not implemented");
}

void columnstore_index_validate_scan(Relation table_rel, Relation index_rel, struct IndexInfo *index_info,
                                     Snapshot snapshot, struct ValidateIndexState *state) {
    elog(ERROR, "columnstore_index_validate_scan not implemented");
}

uint64 columnstore_relation_size(Relation rel, ForkNumber forkNumber) {
    return 0;
}

bool columnstore_relation_needs_toast_table(Relation rel) {
    return false;
}

void columnstore_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples,
                                        double *allvisfrac) {
    // TODO: elog(NOTICE, "columnstore_relation_estimate_size not fully implemented");
}

bool columnstore_scan_sample_next_block(TableScanDesc scan, struct SampleScanState *scanstate) {
    elog(ERROR, "columnstore_scan_sample_next_block not implemented");
}

bool columnstore_scan_sample_next_tuple(TableScanDesc scan, struct SampleScanState *scanstate, TupleTableSlot *slot) {
    elog(ERROR, "columnstore_scan_sample_next_tuple not implemented");
}

const TableAmRoutine columnstore_routine = {
    T_TableAmRoutine,

    columnstore_slot_callbacks,

    columnstore_scan_begin,
    columnstore_scan_end,
    columnstore_scan_rescan,
    columnstore_scan_getnextslot,

    NULL /*scan_set_tidrange*/,
    NULL /*scan_getnextslot_tidrange*/,

    columnstore_parallelscan_estimate,
    columnstore_parallelscan_initialize,
    columnstore_parallelscan_reinitialize,

    columnstore_index_fetch_begin,
    columnstore_index_fetch_reset,
    columnstore_index_fetch_end,
    columnstore_index_fetch_tuple,

    columnstore_tuple_fetch_row_version,
    columnstore_tuple_tid_valid,
    columnstore_tuple_get_latest_tid,
    columnstore_tuple_satisfies_snapshot,
    columnstore_index_delete_tuples,

    columnstore_tuple_insert,
    columnstore_tuple_insert_speculative,
    columnstore_tuple_complete_speculative,
    columnstore_multi_insert,
    columnstore_tuple_delete,
    columnstore_tuple_update,
    columnstore_tuple_lock,
    NULL /*finish_bulk_insert*/,

#if PG_VERSION_NUM >= 160000
    columnstore_relation_set_new_filelocator,
#else
    columnstore_relation_set_new_filenode,
#endif
    columnstore_relation_nontransactional_truncate,
    columnstore_relation_copy_data,
    columnstore_relation_copy_for_cluster,
    columnstore_relation_vacuum,
    columnstore_scan_analyze_next_block,
    columnstore_scan_analyze_next_tuple,
    columnstore_index_build_range_scan,
    columnstore_index_validate_scan,

    columnstore_relation_size,
    columnstore_relation_needs_toast_table,
    NULL /*relation_toast_am*/,
    NULL /*relation_fetch_toast_slice*/,

    columnstore_relation_estimate_size,

    NULL /*scan_bitmap_next_block*/,
    NULL /*scan_bitmap_next_tuple*/,
    columnstore_scan_sample_next_block,
    columnstore_scan_sample_next_tuple
};

extern "C" {
PG_FUNCTION_INFO_V1(columnstore_handler);
Datum columnstore_handler(PG_FUNCTION_ARGS) {
    PG_RETURN_POINTER(&columnstore_routine);
}
}

bool IsColumnstoreTable(Relation rel) {
    return rel->rd_tableam == &columnstore_routine;
}

bool IsColumnstoreTable(Oid oid) {
    if (oid == InvalidOid) {
        return false;
    }

    auto rel = RelationIdGetRelation(oid);
    bool result = IsColumnstoreTable(rel);
    RelationClose(rel);
    return result;
}
