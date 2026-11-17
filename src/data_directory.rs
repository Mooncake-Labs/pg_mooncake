//! `mooncake.data_directory`: the directory moonlink keeps its files and its RPC socket in.
//!
//! The default, `pg_mooncake`, is relative to the Postgres data directory (the server's working directory). moonlink's
//! file names are longer than the 99 characters Postgres' tar format allows, so pg_basebackup fails on a data
//! directory that contains them; a directory outside it avoids that.

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::ffi::{CStr, CString};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const DEFAULT: &str = "pg_mooncake";

static DATA_DIRECTORY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"pg_mooncake"));

/// The setting, read once at load: it can't change while the server runs, and the moonlink thread and DuckDB's
/// threads, which need it too, can't read GUCs.
static VALUE: OnceLock<String> = OnceLock::new();

/// sun_path holds 108 bytes, including the terminating NUL.
const MAX_SOCKET_PATH_LEN: usize = 107;

/// Group write and any access for others, which Postgres refuses on its data directory too (checkDataDir).
const DISALLOWED_MODE_BITS: u32 = 0o027;

/// Called by the postmaster, while it loads shared_preload_libraries.
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
    validate();
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

fn config_error(message: String, hint: Option<&str>) -> ! {
    let mut report = ErrorReport::new(
        PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR,
        message,
        function_name!(),
    );
    if let Some(hint) = hint {
        report = report.set_hint(hint);
    }
    report.report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}

fn validate() {
    let dir = data_directory();
    if dir.is_empty() {
        config_error("mooncake.data_directory must not be empty".to_owned(), None);
    }
    let socket = socket_path();
    if socket.len() > MAX_SOCKET_PATH_LEN {
        config_error(
            format!(
                "mooncake.data_directory \"{dir}\" is too long: the moonlink socket path {socket} has {} bytes, at most {MAX_SOCKET_PATH_LEN} are allowed",
                socket.len()
            ),
            None,
        );
    }
    let path = Path::new(&dir);
    let read_error = |e: std::io::Error| -> ! {
        config_error(
            format!("could not read mooncake.data_directory \"{dir}\": {e}"),
            None,
        )
    };
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            if let Err(e) = std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(path)
            {
                config_error(
                    format!("could not create mooncake.data_directory \"{dir}\": {e}"),
                    None,
                );
            }
            std::fs::symlink_metadata(path).unwrap_or_else(|e| read_error(e))
        }
        Err(e) => read_error(e),
    };
    if metadata.file_type().is_symlink() {
        config_error(
            format!("mooncake.data_directory \"{dir}\" is a symbolic link"),
            Some("Set it to the directory the link points to."),
        );
    }
    if !metadata.is_dir() {
        config_error(
            format!("mooncake.data_directory \"{dir}\" is not a directory"),
            None,
        );
    }
    let uid = unsafe { libc::geteuid() };
    if metadata.uid() != uid {
        config_error(
            format!(
                "mooncake.data_directory \"{dir}\" is owned by user id {}, not by the server's user (id {uid})",
                metadata.uid()
            ),
            Some("The server's user must own the directory."),
        );
    }
    if metadata.mode() & DISALLOWED_MODE_BITS != 0 {
        config_error(
            format!(
                "mooncake.data_directory \"{dir}\" has invalid permissions {:04o}",
                metadata.mode() & 0o7777
            ),
            Some("Permissions should be u=rwx (0700) or u=rwx,g=rx (0750)."),
        );
    }
    let dir_c = CString::new(dir.as_str()).unwrap_or_else(|_| {
        config_error(
            "mooncake.data_directory must not contain a NUL byte".to_owned(),
            None,
        )
    });
    if unsafe { libc::access(dir_c.as_ptr(), libc::W_OK | libc::X_OK) } != 0 {
        config_error(
            format!(
                "mooncake.data_directory \"{dir}\" is not writable by the server: {}",
                std::io::Error::last_os_error()
            ),
            None,
        );
    }
    let data_dir = canonical(Path::new(&postgres_data_directory()));
    let canonical_dir = canonical(path);
    if data_dir.starts_with(&canonical_dir) {
        config_error(
            format!(
                "mooncake.data_directory \"{dir}\" must not be the data directory or contain it"
            ),
            None,
        );
    }
    // Without the background worker, moonlink runs on its own, owns the directory and is expected to be listening.
    if cfg!(feature = "bgworker") {
        lock(path, &dir);
        if socket_accepts_connections(&socket) {
            in_use_error(
                &dir,
                format!("Its moonlink socket {socket} accepts connections."),
            );
        }
    }
    // ./pg_mooncake, or an absolute path to it, is the default too.
    if canonical_dir.starts_with(&data_dir) && canonical_dir != data_dir.join(DEFAULT) {
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_WARNING,
            format!(
                "mooncake.data_directory \"{dir}\" is inside the data directory, so pg_basebackup will fail on moonlink's file names"
            ),
            function_name!(),
        )
        .set_hint("Use a directory outside the data directory.")
        .report(PgLogLevel::WARNING);
    }
}

fn postgres_data_directory() -> String {
    unsafe { CStr::from_ptr(pg_sys::DataDir) }
        .to_string_lossy()
        .into_owned()
}

fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path))
}

fn in_use_error(dir: &str, detail: String) -> ! {
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_OBJECT_IN_USE,
        format!("mooncake.data_directory \"{dir}\" is in use by another server"),
        function_name!(),
    )
    .set_detail(detail)
    .set_hint(
        "Give each server its own mooncake.data_directory, also a copy made with pg_basebackup.",
    )
    .report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}

/// Locks the directory for this server with flock on `moonlink.lock`, which also names the holder. The file stays open
/// (and locked) for the postmaster's lifetime. Its children inherit it, so after a postmaster crash the lock is held
/// until the last of them has exited, and the kernel releases it however the processes end. Unlike an fcntl lock, it
/// isn't released when the process closes another descriptor of the same file.
fn lock(path: &Path, dir: &str) {
    let lock_path = path.join("moonlink.lock");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .unwrap_or_else(|e| {
            config_error(format!("could not open {}: {e}", lock_path.display()), None)
        });
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EWOULDBLOCK) {
            config_error(format!("could not lock {}: {e}", lock_path.display()), None);
        }
        let mut holder = String::new();
        let _ = file.read_to_string(&mut holder);
        let mut lines = holder.lines();
        let detail = match (lines.next(), lines.next()) {
            (Some(pid), Some(data_dir)) => format!(
                "{} is locked by the server with data directory {data_dir} (postmaster PID {pid}).",
                lock_path.display()
            ),
            _ => format!("{} is locked by another server.", lock_path.display()),
        };
        in_use_error(dir, detail);
    }
    let holder = format!(
        "{}\n{}\n",
        unsafe { libc::getpid() },
        postgres_data_directory()
    );
    if let Err(e) = file
        .set_len(0)
        .and_then(|()| file.write_all(holder.as_bytes()))
    {
        config_error(
            format!("could not write {}: {e}", lock_path.display()),
            None,
        );
    }
    std::mem::forget(file);
}

/// Whether a server listens on the moonlink socket. The connect doesn't block: a listener with a full backlog counts
/// as listening, and an in-progress connect gets half a second.
fn socket_accepts_connections(socket: &str) -> bool {
    unsafe {
        let fd = libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        );
        if fd < 0 {
            return false;
        }
        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (dst, src) in addr.sun_path.iter_mut().zip(socket.as_bytes()) {
            *dst = *src as libc::c_char;
        }
        let rc = libc::connect(
            fd,
            std::ptr::addr_of!(addr).cast(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        );
        let listening = if rc == 0 {
            true
        } else {
            match std::io::Error::last_os_error().raw_os_error() {
                Some(libc::EAGAIN) => true,
                Some(libc::EINPROGRESS) => {
                    let mut pollfd = libc::pollfd {
                        fd,
                        events: libc::POLLOUT,
                        revents: 0,
                    };
                    let mut error: libc::c_int = 0;
                    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                    libc::poll(&mut pollfd, 1, 500) > 0
                        && libc::getsockopt(
                            fd,
                            libc::SOL_SOCKET,
                            libc::SO_ERROR,
                            std::ptr::addr_of_mut!(error).cast(),
                            &mut len,
                        ) == 0
                        && error == 0
                }
                _ => false,
            }
        };
        libc::close(fd);
        listening
    }
}
