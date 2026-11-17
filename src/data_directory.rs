//! `mooncake.data_directory`: the directory moonlink keeps its files and its RPC socket in.
//!
//! The default, `pg_mooncake`, is relative to the Postgres data directory (the server's working directory). moonlink's
//! file names are longer than the 99 characters Postgres' tar format allows, so pg_basebackup fails on a data
//! directory that contains them; a directory outside it avoids that.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::{CStr, CString};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const DEFAULT: &CStr = c"pg_mooncake";

static DATA_DIRECTORY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(DEFAULT));

/// The setting, read once at load: it can't change while the server runs, and the moonlink thread and DuckDB's
/// threads, which need it too, can't read GUCs.
static VALUE: OnceLock<String> = OnceLock::new();

/// sun_path holds 108 bytes, including the terminating NUL.
const MAX_SOCKET_PATH_LEN: usize = 107;

pub(crate) fn init() {
    GucRegistry::define_string_guc(
        c"mooncake.data_directory",
        c"Directory for moonlink's files and RPC socket.",
        c"A relative path is relative to the data directory. Set it to a directory outside the data directory for \
          pg_basebackup to work.",
        &DATA_DIRECTORY,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    let value = DATA_DIRECTORY
        .get()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let _ = VALUE.set(value);
    // Only the postmaster checks it, once: a backend that loads the library later has nothing to set up.
    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        validate();
    }
}

/// The setting as given: relative paths stay relative to the data directory, which is every server process's working
/// directory.
pub(crate) fn data_directory() -> String {
    VALUE
        .get()
        .expect("mooncake.data_directory is read in _PG_init")
        .clone()
}

pub(crate) fn socket_path() -> String {
    Path::new(&data_directory())
        .join("moonlink.sock")
        .to_string_lossy()
        .into_owned()
}

fn validate() {
    let dir = data_directory();
    if dir.is_empty() {
        error!("mooncake.data_directory must not be empty");
    }
    let socket = socket_path();
    if socket.len() > MAX_SOCKET_PATH_LEN {
        error!(
            "mooncake.data_directory is too long: the moonlink socket path {socket} has {} bytes, at most {MAX_SOCKET_PATH_LEN} are allowed",
            socket.len()
        );
    }
    let path = Path::new(&dir);
    if let Err(e) = std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
    {
        error!("could not create mooncake.data_directory \"{dir}\": {e}");
    }
    let metadata = std::fs::metadata(path)
        .unwrap_or_else(|e| error!("could not read mooncake.data_directory \"{dir}\": {e}"));
    if !metadata.is_dir() {
        error!("mooncake.data_directory \"{dir}\" is not a directory");
    }
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid {
        error!(
            "mooncake.data_directory \"{dir}\" is owned by user id {}, not by the server's user (id {uid})",
            metadata.uid()
        );
    }
    let dir_c = CString::new(dir.as_str())
        .unwrap_or_else(|_| error!("mooncake.data_directory must not contain a NUL byte"));
    if unsafe { libc::access(dir_c.as_ptr(), libc::W_OK | libc::X_OK) } != 0 {
        error!(
            "mooncake.data_directory \"{dir}\" is not writable by the server: {}",
            std::io::Error::last_os_error()
        );
    }
    // moonlink takes over the socket path, so a second server on the same directory would cut the first one off.
    // Without the background worker, moonlink runs on its own and is expected to be listening already.
    if cfg!(feature = "bgworker") && std::os::unix::net::UnixStream::connect(&socket).is_ok() {
        error!(
            "mooncake.data_directory \"{dir}\" is in use by another server: {socket} accepts connections\n\
             HINT: Give each server its own mooncake.data_directory, also a copy made with pg_basebackup."
        );
    }
    if path != Path::new(DEFAULT.to_str().unwrap()) && inside_data_directory(path) {
        warning!(
            "mooncake.data_directory \"{dir}\" is inside the data directory, so pg_basebackup will fail on moonlink's \
             file names\nHINT: Use a directory outside the data directory."
        );
    }
}

fn inside_data_directory(path: &Path) -> bool {
    let data_dir = unsafe { CStr::from_ptr(pg_sys::DataDir) }
        .to_string_lossy()
        .into_owned();
    let canonical = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| PathBuf::from(p));
    canonical(path).starts_with(canonical(Path::new(&data_dir)))
}
