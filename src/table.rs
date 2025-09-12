use core::ffi::{c_char, c_int, c_void};
use pgrx::prelude::*;
use std::ffi::CString;

extern "C" {
    fn RegisterDuckdbTableAm(name: *const c_char, am: *const pg_sys::TableAmRoutine) -> bool;
}

#[no_mangle]
static mut MOONCAKE_AM: pg_sys::TableAmRoutine = pg_sys::TableAmRoutine {
    type_: pg_sys::NodeTag::T_TableAmRoutine,
    slot_callbacks: Some(mooncake_slot_callbacks),
    scan_begin: Some(mooncake_scan_begin),
    scan_end: Some(mooncake_scan_end),
    scan_rescan: Some(mooncake_scan_rescan),
    scan_getnextslot: Some(mooncake_scan_getnextslot),
    scan_set_tidrange: Some(mooncake_scan_set_tidrange),
    scan_getnextslot_tidrange: Some(mooncake_scan_getnextslot_tidrange),
    parallelscan_estimate: Some(mooncake_parallelscan_estimate),
    parallelscan_initialize: Some(mooncake_parallelscan_initialize),
    parallelscan_reinitialize: Some(mooncake_parallelscan_reinitialize),
    index_fetch_begin: Some(mooncake_index_fetch_begin),
    index_fetch_reset: Some(mooncake_index_fetch_reset),
    index_fetch_end: Some(mooncake_index_fetch_end),
    index_fetch_tuple: Some(mooncake_index_fetch_tuple),
    tuple_fetch_row_version: Some(mooncake_tuple_fetch_row_version),
    tuple_tid_valid: Some(mooncake_tuple_tid_valid),
    tuple_get_latest_tid: Some(mooncake_tuple_get_latest_tid),
    tuple_satisfies_snapshot: Some(mooncake_tuple_satisfies_snapshot),
    index_delete_tuples: Some(mooncake_index_delete_tuples),
    tuple_insert: Some(mooncake_tuple_insert),
    tuple_insert_speculative: Some(mooncake_tuple_insert_speculative),
    tuple_complete_speculative: Some(mooncake_tuple_complete_speculative),
    multi_insert: Some(mooncake_multi_insert),
    tuple_delete: Some(mooncake_tuple_delete),
    tuple_update: Some(mooncake_tuple_update),
    tuple_lock: Some(mooncake_tuple_lock),
    finish_bulk_insert: Some(mooncake_finish_bulk_insert),
    #[cfg(any(feature = "pg14", feature = "pg15"))]
    relation_set_new_filenode: Some(mooncake_relation_set_new_filenode),
    #[cfg(any(feature = "pg16", feature = "pg17"))]
    relation_set_new_filelocator: Some(mooncake_relation_set_new_filelocator),
    relation_nontransactional_truncate: Some(mooncake_relation_nontransactional_truncate),
    relation_copy_data: Some(mooncake_relation_copy_data),
    relation_copy_for_cluster: Some(mooncake_relation_copy_for_cluster),
    relation_vacuum: Some(mooncake_relation_vacuum),
    scan_analyze_next_block: Some(mooncake_scan_analyze_next_block),
    scan_analyze_next_tuple: Some(mooncake_scan_analyze_next_tuple),
    index_build_range_scan: Some(mooncake_index_build_range_scan),
    index_validate_scan: Some(mooncake_index_validate_scan),
    relation_size: Some(mooncake_relation_size),
    relation_needs_toast_table: Some(mooncake_relation_needs_toast_table),
    relation_toast_am: Some(mooncake_relation_toast_am),
    relation_fetch_toast_slice: Some(mooncake_relation_fetch_toast_slice),
    relation_estimate_size: Some(mooncake_relation_estimate_size),
    scan_bitmap_next_block: Some(mooncake_scan_bitmap_next_block),
    scan_bitmap_next_tuple: Some(mooncake_scan_bitmap_next_tuple),
    scan_sample_next_block: Some(mooncake_scan_sample_next_block),
    scan_sample_next_tuple: Some(mooncake_scan_sample_next_tuple),
};

pub(crate) fn init() {
    let name = CString::new("mooncake").expect("name should not contain an internal 0 byte");
    let res = unsafe { RegisterDuckdbTableAm(name.as_ptr(), std::ptr::addr_of!(MOONCAKE_AM)) };
    assert!(res, "error registering mooncake table AM");
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake_am_handler(internal) RETURNS table_am_handler LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD mooncake TYPE TABLE HANDLER mooncake_am_handler;
")]
fn mooncake_am_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::TableAmRoutine> {
    unsafe { PgBox::from_pg(std::ptr::addr_of_mut!(MOONCAKE_AM)) }
}

#[pg_guard]
extern "C-unwind" fn mooncake_slot_callbacks(
    _rel: pg_sys::Relation,
) -> *const pg_sys::TupleTableSlotOps {
    unsafe { &pg_sys::TTSOpsMinimalTuple }
}

struct MooncakeScanDescData {
    rs_base: pg_sys::TableScanDescData,
}

type MooncakeScanDesc = *mut MooncakeScanDescData;

#[pg_guard]
extern "C-unwind" fn mooncake_scan_begin(
    rel: pg_sys::Relation,
    snapshot: pg_sys::Snapshot,
    nkeys: c_int,
    _key: *mut pg_sys::ScanKeyData,
    pscan: pg_sys::ParallelTableScanDesc,
    flags: pg_sys::uint32,
) -> pg_sys::TableScanDesc {
    let mut scan = unsafe { PgBox::<MooncakeScanDescData>::alloc() };
    scan.rs_base.rs_rd = rel;
    scan.rs_base.rs_snapshot = snapshot;
    scan.rs_base.rs_nkeys = nkeys;
    scan.rs_base.rs_flags = flags;
    scan.rs_base.rs_parallel = pscan;
    scan.into_pg() as pg_sys::TableScanDesc
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_end(scan: pg_sys::TableScanDesc) {
    unsafe { PgBox::<MooncakeScanDescData>::from_rust(scan as MooncakeScanDesc) };
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_rescan(
    _scan: pg_sys::TableScanDesc,
    _key: *mut pg_sys::ScanKeyData,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
    unimplemented!("mooncake_scan_rescan");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_getnextslot(
    _scan: pg_sys::TableScanDesc,
    _direction: pg_sys::ScanDirection::Type,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_scan_getnextslot");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_set_tidrange(
    _scan: pg_sys::TableScanDesc,
    _mintid: pg_sys::ItemPointer,
    _maxtid: pg_sys::ItemPointer,
) {
    unimplemented!("mooncake_scan_set_tidrange");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_getnextslot_tidrange(
    _scan: pg_sys::TableScanDesc,
    _direction: pg_sys::ScanDirection::Type,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_scan_getnextslot_tidrange");
}

#[pg_guard]
extern "C-unwind" fn mooncake_parallelscan_estimate(_rel: pg_sys::Relation) -> pg_sys::Size {
    unimplemented!("mooncake_parallelscan_estimate");
}

#[pg_guard]
extern "C-unwind" fn mooncake_parallelscan_initialize(
    _rel: pg_sys::Relation,
    _pscan: pg_sys::ParallelTableScanDesc,
) -> pg_sys::Size {
    unimplemented!("mooncake_parallelscan_initialize");
}

#[pg_guard]
extern "C-unwind" fn mooncake_parallelscan_reinitialize(
    _rel: pg_sys::Relation,
    _pscan: pg_sys::ParallelTableScanDesc,
) {
    unimplemented!("mooncake_parallelscan_reinitialize");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_fetch_begin(
    _rel: pg_sys::Relation,
) -> *mut pg_sys::IndexFetchTableData {
    unimplemented!("mooncake_index_fetch_begin");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_fetch_reset(_data: *mut pg_sys::IndexFetchTableData) {
    unimplemented!("mooncake_index_fetch_reset");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_fetch_end(_data: *mut pg_sys::IndexFetchTableData) {
    unimplemented!("mooncake_index_fetch_end");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_fetch_tuple(
    _scan: *mut pg_sys::IndexFetchTableData,
    _tid: pg_sys::ItemPointer,
    _snapshot: pg_sys::Snapshot,
    _slot: *mut pg_sys::TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    unimplemented!("mooncake_index_fetch_tuple");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_fetch_row_version(
    _rel: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    _snapshot: pg_sys::Snapshot,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_tuple_fetch_row_version");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_tid_valid(
    _scan: pg_sys::TableScanDesc,
    _tid: pg_sys::ItemPointer,
) -> bool {
    unimplemented!("mooncake_tuple_tid_valid");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_get_latest_tid(
    _scan: pg_sys::TableScanDesc,
    _tid: pg_sys::ItemPointer,
) {
    unimplemented!("mooncake_tuple_get_latest_tid");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_satisfies_snapshot(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _snapshot: pg_sys::Snapshot,
) -> bool {
    unimplemented!("mooncake_tuple_satisfies_snapshot");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_delete_tuples(
    _rel: pg_sys::Relation,
    _delstate: *mut pg_sys::TM_IndexDeleteOp,
) -> pg_sys::TransactionId {
    unimplemented!("mooncake_index_delete_tuples");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_insert(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    unimplemented!("mooncake_tuple_insert");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_insert_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
    _spec_token: pg_sys::uint32,
) {
    unimplemented!("mooncake_tuple_insert_speculative");
}

#[pg_guard]
extern "C-unwind" fn mooncake_tuple_complete_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _spec_token: pg_sys::uint32,
    _succeeded: bool,
) {
    unimplemented!("mooncake_tuple_complete_speculative");
}

#[pg_guard]
extern "C-unwind" fn mooncake_multi_insert(
    _rel: pg_sys::Relation,
    _slots: *mut *mut pg_sys::TupleTableSlot,
    _nslots: c_int,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    unimplemented!("mooncake_multi_insert");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn mooncake_tuple_delete(
    _rel: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    _cid: pg_sys::CommandId,
    _snapshot: pg_sys::Snapshot,
    _crosscheck: pg_sys::Snapshot,
    _wait: bool,
    _tmfd: *mut pg_sys::TM_FailureData,
    _changing_part: bool,
) -> pg_sys::TM_Result::Type {
    unimplemented!("mooncake_tuple_delete");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "pg14", feature = "pg15"))]
extern "C-unwind" fn mooncake_tuple_update(
    _rel: pg_sys::Relation,
    _otid: pg_sys::ItemPointer,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _snapshot: pg_sys::Snapshot,
    _crosscheck: pg_sys::Snapshot,
    _wait: bool,
    _tmfd: *mut pg_sys::TM_FailureData,
    _lockmode: *mut pg_sys::LockTupleMode::Type,
    _update_indexes: *mut bool,
) -> pg_sys::TM_Result::Type {
    unimplemented!("mooncake_tuple_update");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "pg16", feature = "pg17"))]
extern "C-unwind" fn mooncake_tuple_update(
    _rel: pg_sys::Relation,
    _otid: pg_sys::ItemPointer,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _snapshot: pg_sys::Snapshot,
    _crosscheck: pg_sys::Snapshot,
    _wait: bool,
    _tmfd: *mut pg_sys::TM_FailureData,
    _lockmode: *mut pg_sys::LockTupleMode::Type,
    _update_indexes: *mut pg_sys::TU_UpdateIndexes::Type,
) -> pg_sys::TM_Result::Type {
    unimplemented!("mooncake_tuple_update");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn mooncake_tuple_lock(
    _rel: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    _snapshot: pg_sys::Snapshot,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _mode: pg_sys::LockTupleMode::Type,
    _wait_policy: pg_sys::LockWaitPolicy::Type,
    _flags: pg_sys::uint8,
    _tmfd: *mut pg_sys::TM_FailureData,
) -> pg_sys::TM_Result::Type {
    unimplemented!("mooncake_tuple_lock");
}

#[pg_guard]
extern "C-unwind" fn mooncake_finish_bulk_insert(_rel: pg_sys::Relation, _options: c_int) {
    unimplemented!("mooncake_finish_bulk_insert");
}

#[pg_guard]
#[cfg(any(feature = "pg14", feature = "pg15"))]
extern "C-unwind" fn mooncake_relation_set_new_filenode(
    _rel: pg_sys::Relation,
    _newrnode: *const pg_sys::RelFileNode,
    _persistence: c_char,
    _freeze_xid: *mut pg_sys::TransactionId,
    _minmulti: *mut pg_sys::MultiXactId,
) {
}

#[pg_guard]
#[cfg(any(feature = "pg16", feature = "pg17"))]
extern "C-unwind" fn mooncake_relation_set_new_filelocator(
    _rel: pg_sys::Relation,
    _newrlocator: *const pg_sys::RelFileLocator,
    _persistence: c_char,
    _freeze_xid: *mut pg_sys::TransactionId,
    _minmulti: *mut pg_sys::MultiXactId,
) {
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_nontransactional_truncate(_rel: pg_sys::Relation) {}

#[pg_guard]
#[cfg(any(feature = "pg14", feature = "pg15"))]
extern "C-unwind" fn mooncake_relation_copy_data(
    _rel: pg_sys::Relation,
    _newrnode: *const pg_sys::RelFileNode,
) {
    unimplemented!("mooncake_relation_copy_data");
}

#[pg_guard]
#[cfg(any(feature = "pg16", feature = "pg17"))]
extern "C-unwind" fn mooncake_relation_copy_data(
    _rel: pg_sys::Relation,
    _newrlocator: *const pg_sys::RelFileLocator,
) {
    unimplemented!("mooncake_relation_copy_data");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn mooncake_relation_copy_for_cluster(
    _old_table: pg_sys::Relation,
    _new_table: pg_sys::Relation,
    _old_index: pg_sys::Relation,
    _use_sort: bool,
    _oldest_xmin: pg_sys::TransactionId,
    _xid_cutoff: *mut pg_sys::TransactionId,
    _multi_cutoff: *mut pg_sys::MultiXactId,
    _num_tuples: *mut f64,
    _tups_vacuumed: *mut f64,
    _tups_recently_dead: *mut f64,
) {
    unimplemented!("mooncake_relation_copy_for_cluster");
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_vacuum(
    _rel: pg_sys::Relation,
    _params: *mut pg_sys::VacuumParams,
    _bstrategy: pg_sys::BufferAccessStrategy,
) {
    unimplemented!("mooncake_relation_vacuum");
}

#[pg_guard]
#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
extern "C-unwind" fn mooncake_scan_analyze_next_block(
    _scan: pg_sys::TableScanDesc,
    _blockno: pg_sys::BlockNumber,
    _bstrategy: pg_sys::BufferAccessStrategy,
) -> bool {
    false
}

#[pg_guard]
#[cfg(feature = "pg17")]
extern "C-unwind" fn mooncake_scan_analyze_next_block(
    _scan: pg_sys::TableScanDesc,
    _stream: *mut pg_sys::ReadStream,
) -> bool {
    false
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_analyze_next_tuple(
    _scan: pg_sys::TableScanDesc,
    _oldest_xmin: pg_sys::TransactionId,
    _liverows: *mut f64,
    _deadrows: *mut f64,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_scan_analyze_next_tuple");
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn mooncake_index_build_range_scan(
    _table_rel: pg_sys::Relation,
    _index_rel: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: pg_sys::BlockNumber,
    _numblocks: pg_sys::BlockNumber,
    _callback: pg_sys::IndexBuildCallback,
    _callback_state: *mut c_void,
    _scan: pg_sys::TableScanDesc,
) -> f64 {
    unimplemented!("mooncake_index_build_range_scan");
}

#[pg_guard]
extern "C-unwind" fn mooncake_index_validate_scan(
    _table_rel: pg_sys::Relation,
    _index_rel: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
    _snapshot: pg_sys::Snapshot,
    _state: *mut pg_sys::ValidateIndexState,
) {
    unimplemented!("mooncake_index_validate_scan");
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_size(
    _rel: pg_sys::Relation,
    _fork_number: pg_sys::ForkNumber::Type,
) -> pg_sys::uint64 {
    0
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_needs_toast_table(_rel: pg_sys::Relation) -> bool {
    false
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_toast_am(_rel: pg_sys::Relation) -> pg_sys::Oid {
    unimplemented!("mooncake_relation_toast_am");
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_fetch_toast_slice(
    _toastrel: pg_sys::Relation,
    _valueid: pg_sys::Oid,
    _attrsize: pg_sys::int32,
    _sliceoffset: pg_sys::int32,
    _slicelength: pg_sys::int32,
    _result: *mut pg_sys::varlena,
) {
    unimplemented!("mooncake_relation_fetch_toast_slice");
}

#[pg_guard]
extern "C-unwind" fn mooncake_relation_estimate_size(
    _rel: pg_sys::Relation,
    attr_widths: *mut pg_sys::int32,
    pages: *mut pg_sys::BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    if !attr_widths.is_null() {
        unsafe { *attr_widths = 0 };
    }
    if !pages.is_null() {
        unsafe { *pages = 0 };
    }
    if !tuples.is_null() {
        unsafe { *tuples = 0.0 };
    }
    if !allvisfrac.is_null() {
        unsafe { *allvisfrac = 0.0 };
    }
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_bitmap_next_block(
    _scan: pg_sys::TableScanDesc,
    _tbmres: *mut pg_sys::TBMIterateResult,
) -> bool {
    unimplemented!("mooncake_scan_bitmap_next_block");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_bitmap_next_tuple(
    _scan: pg_sys::TableScanDesc,
    _tbmres: *mut pg_sys::TBMIterateResult,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_scan_bitmap_next_tuple");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_sample_next_block(
    _scan: pg_sys::TableScanDesc,
    _scanstate: *mut pg_sys::SampleScanState,
) -> bool {
    unimplemented!("mooncake_scan_sample_next_block");
}

#[pg_guard]
extern "C-unwind" fn mooncake_scan_sample_next_tuple(
    _scan: pg_sys::TableScanDesc,
    _scanstate: *mut pg_sys::SampleScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    unimplemented!("mooncake_scan_sample_next_tuple");
}
