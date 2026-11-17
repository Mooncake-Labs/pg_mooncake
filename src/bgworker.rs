use moonlink_service::{start_with_config, ServiceConfig};
use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::prelude::*;
use std::time::Duration;

pub(crate) fn init() {
    BackgroundWorkerBuilder::new("moonlink")
        .set_library("pg_mooncake")
        .set_function("moonlink_main")
        .enable_shmem_access(None)
        // Not on a standby: moonlink can't replicate from a read-only server, and its attempts would error the
        // mirrors. Postgres starts the worker once recovery ends, at startup or on promotion.
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(15)))
        .load();
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn moonlink_main(_arg: pg_sys::Datum) {
    std::env::set_var("RUST_BACKTRACE", "1");
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    // moonlink's own SIGTERM handling (tokio::signal) chains to the pgrx handler above, which calls into Postgres and
    // panics on any thread but this one; the panic can also lose the shutdown request. A signal sent to the process
    // goes to a thread that doesn't block it, so every moonlink thread blocks the shutdown signals and only this
    // thread, which just waits for moonlink, receives them. Threads inherit the mask of the thread that creates them:
    // the moonlink thread and the runtime's workers are created while this thread blocks the signals, and
    // on_thread_start also covers blocking-pool threads started later. SIGQUIT (immediate shutdown) stays deliverable
    // everywhere.
    //
    // moonlink runs under block_on on its own thread, not as a task on the runtime: as a task, its shutdown races make
    // fast stops crash the worker far more often.
    let original_mask = block_shutdown_signals();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            block_shutdown_signals();
        })
        .build()
        .expect("failed to build the moonlink runtime");
    // The main thread's stack size: moonlink's startup and recovery run inline in this future, and have overflowed a
    // smaller stack in debug builds.
    let moonlink = std::thread::Builder::new()
        .name("moonlink".to_owned())
        .stack_size(MOONLINK_THREAD_STACK_SIZE)
        .spawn(move || runtime.block_on(start_with_config(service_config())))
        .expect("failed to start the moonlink thread");
    set_signal_mask(&original_mask);
    wait_for_moonlink(&moonlink);
    // An error is raised here, under pg_guard, so it is reported like any other worker error.
    match moonlink.join() {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    // moonlink returns only after SIGTERM. Postgres restarts a background worker only after a non-zero exit, so a
    // SIGTERM outside a server shutdown (a kill from outside Postgres: the worker isn't in the ProcArray, so
    // pg_terminate_backend can't reach it) would otherwise leave every mirror without moonlink until the next server
    // restart. A shutdown stays a clean exit.
    if !postmaster_stopping() {
        log!("moonlink stopped on SIGTERM outside a server shutdown; exiting with code 1 so that it is restarted");
        unsafe { pg_sys::proc_exit(1) };
    }
}

const MOONLINK_THREAD_STACK_SIZE: usize = 8 << 20;

/// Waits for the moonlink thread to finish. A SIGTERM that arrives before moonlink has installed its own handler
/// reaches only pgrx's, and moonlink would never stop: once one has arrived, raise it again on this thread (the only
/// one that takes it) until moonlink is gone. moonlink waits for SIGTERM once, so repeats are harmless.
fn wait_for_moonlink(moonlink: &std::thread::JoinHandle<moonlink_service::Result<()>>) {
    const POLL_INTERVAL: Duration = Duration::from_millis(200);
    const RAISE_INTERVAL: Duration = Duration::from_secs(2);
    let mut last_raise: Option<std::time::Instant> = None;
    let mut sigterm = false;
    while !moonlink.is_finished() {
        std::thread::sleep(POLL_INTERVAL);
        sigterm |= BackgroundWorker::sigterm_received();
        if sigterm
            && !moonlink.is_finished()
            && last_raise.is_none_or(|at| at.elapsed() >= RAISE_INTERVAL)
        {
            unsafe { libc::raise(libc::SIGTERM) };
            last_raise = Some(std::time::Instant::now());
        }
    }
}

/// Whether the postmaster is shutting down. It records "stopping" in postmaster.pid (the PM_STATUS line) for smart,
/// fast and immediate shutdowns before it signals any child, so a worker that got SIGTERM because of a shutdown
/// always sees it. Unreadable counts as not stopping: an extra restart is better than a worker that stays down.
fn postmaster_stopping() -> bool {
    const LOCK_FILE_LINE_PM_STATUS: usize = 8;
    let data_dir = unsafe { std::ffi::CStr::from_ptr(pg_sys::DataDir) }.to_string_lossy();
    std::fs::read_to_string(std::path::Path::new(data_dir.as_ref()).join("postmaster.pid"))
        .ok()
        .and_then(|pid_file| {
            pid_file
                .lines()
                .nth(LOCK_FILE_LINE_PM_STATUS - 1)
                .map(|status| status.trim() == "stopping")
        })
        .unwrap_or(false)
}

/// Blocks SIGTERM, SIGHUP and SIGINT on the calling thread and returns its previous signal mask.
fn block_shutdown_signals() -> libc::sigset_t {
    unsafe {
        let mut signals: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut signals);
        for signal in [libc::SIGTERM, libc::SIGHUP, libc::SIGINT] {
            libc::sigaddset(&mut signals, signal);
        }
        let mut previous: libc::sigset_t = std::mem::zeroed();
        let rc = libc::pthread_sigmask(libc::SIG_BLOCK, &signals, &mut previous);
        assert_eq!(rc, 0, "pthread_sigmask failed");
        previous
    }
}

fn set_signal_mask(mask: &libc::sigset_t) {
    let rc = unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, mask, std::ptr::null_mut()) };
    assert_eq!(rc, 0, "pthread_sigmask failed");
}

fn service_config() -> ServiceConfig {
    ServiceConfig {
        base_path: crate::data_directory::data_directory(),
    }
}
