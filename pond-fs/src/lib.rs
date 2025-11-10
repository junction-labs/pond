mod fuse;
mod log;

use log::init_logging;

use bytesize::ByteSize;
use clap::{Parser, Subcommand, value_parser};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use nix::{
    poll::{PollFd, PollFlags},
    sys::{signal::Signal, wait::waitpid},
    unistd::ForkResult,
};
use pond::{Client, FileType, Ino, Version, Volume};
use std::{
    fs::File,
    io::{Read, Write},
    os::fd::{AsFd, AsRawFd, OwnedFd},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};

use crate::fuse::Pond;

/// Like eprintln but doesn't panic on error.
///
/// Exported for use in main.rs.
#[macro_export]
macro_rules! write_stderr {
    ($($args:tt)*) => {{
            use std::io::Write as _;
            let _ = writeln!(std::io::stderr(), $($args)*);
    }}
}

#[derive(Parser)]
#[clap(version = version())]
pub struct Args {
    #[command(subcommand)]
    pub cmd: Cmd,

    /// Include the full error chain for CLI error output. For a fully detailed backtrace, set
    /// RUST_BACKTRACE=1.
    #[clap(long, default_value_t = false)]
    pub backtrace: bool,
}

fn version() -> String {
    let pkg_version = option_env!("CARGO_PKG_VERSION").unwrap_or("dev");
    let git_sha = option_env!("POND_GIT_SHA");
    match git_sha {
        Some(sha) => format!("pond {pkg_version} ({sha})"),
        None => format!("pond {pkg_version}"),
    }
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Mount a volume as a local filesystem.
    Mount(MountArgs),

    /// List the available versions of a volume.
    Versions {
        /// The URL of the volume.
        volume: String,
    },

    /// List the contents of a volume.
    List {
        /// The URL of the volume.
        volume: String,

        /// The version of the volume. Defaults to the lexographically greatest
        /// version in the volume.
        #[clap(long)]
        version: Option<String>,
    },

    /// Create a new volume from a local directory.
    ///
    /// Creates a new volume by copying files and directories from a local path
    /// into object storage. Regular files and directories will be copied, any
    /// symlinks will be ignored and not treated as directories.
    Create {
        /// The local directory to create the volume from.
        dir: String,

        /// The URL of the new volume. Must be a valid object storage URL.
        volume: String,

        /// The version to create the volume with.
        version: String,
    },
}

#[derive(Parser)]
pub struct MountArgs {
    /// Run Pond in a background process. By default, Pond runs in the
    /// foreground. When running in the background, logging to stdout/stderr is
    /// disabled.
    ///
    /// *NOTE* - If you're running Pond as a system service we strongly
    /// recommend running it under your system's process supervisor instead of
    /// using this option.
    #[clap(long, short, default_value_t = true, action = clap::ArgAction::Set)]
    pub background: bool,

    /// The number of worker threads threads to run in the FUSE. Worker threads
    /// are used for making network requests. There must be at least one FUSE
    /// thread.
    #[clap(
        long,
        default_value = "2",
        value_parser = clap::value_parser!(u64).range(1..=2048),
    )]
    pub worker_threads: u64,

    /// The maximum number of blocking threads that will be spawned when
    /// blocking on the local filesystem, must be between 8 and 2048. These
    /// threads are used for any local writes.
    ///
    /// Blocking threads are pooled, and terminated when not in use. They do not
    /// take up additional resources. Setting this value lower than the default
    /// of 512 is not recommended.
    #[clap(
        long,
        default_value = "512",
        value_parser = clap::value_parser!(u64).range(8..=2048),
    )]
    pub blocking_threads: u64,

    /// Run with debug logging enabled. By default Pond only logs high-priority
    /// messages, but stays mostly silent otherwise. This is mostly useful if
    /// you're debugging an issue.
    #[clap(long)]
    pub debug: bool,

    /// Log to a log file at the given path. The parent directory must exist
    /// and be writable by this process.
    ///
    /// This does not disable logging to stdout and stderr.
    #[clap(long)]
    pub log_file: Option<PathBuf>,

    /// All all other users of the system to access the filesystem.
    ///
    /// To use this, `user_allow_other` must be set in your /etc/fuse.conf.
    ///
    /// By convention, this is disabled by default in FUSE implementations. See
    /// the libfuse wiki for an explanation of why.
    ///
    /// https://github.com/libfuse/libfuse/wiki/FAQ#why-dont-other-users-have-access-to-the-mounted-filesystem
    #[clap(long)]
    pub allow_other: bool,

    /// Automatically unmount the filesystem if this process exits for any reason.
    ///
    /// If you are not running Pond as root, `user_allow_other` must be set in your /etc/fuse.conf
    /// to enable auto_unmount.
    #[clap(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub auto_unmount: std::primitive::bool,

    /// The timeout in seconds for which name lookups and file/directory attributes will be cached.
    #[clap(long, default_value = "1.0", value_name = "SECONDS", value_parser = parse_duration_secs)]
    pub kernel_cache_timeout: Duration,

    #[command(flatten)]
    pub read_behavior: ReadBehaviorArgs,

    /// The URL of the volume to mount.
    pub volume: String,

    /// The local directory to mount the volume at. Must already exist.
    pub mountpoint: PathBuf,

    /// The version of the volume to mount. Defaults to the lexographically
    /// greatest version in the volume if not specified.
    #[clap(long)]
    pub version: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct ReadBehaviorArgs {
    /// Maximum size of the chunk cache.
    #[clap(long, default_value = "10GiB", value_parser = value_parser!(ByteSize))]
    max_cache_size: ByteSize,

    /// The size of the chunk we fetch from object storage in a single request.
    /// It's also the size of the buffers we store within a single cache entry.
    #[clap(long, default_value = "8MiB", value_parser = value_parser!(ByteSize))]
    chunk_size: ByteSize,

    /// Size of readahead. If you're reading a file at byte 0, Pond will
    /// pre-fetch the bytes up to `readahead_size` in parallel.
    #[clap(long, default_value = "32MiB", value_parser = value_parser!(ByteSize))]
    readahead_size: ByteSize,
}

fn parse_duration_secs(s: &str) -> anyhow::Result<Duration> {
    let secs: f64 = s.parse()?;
    Ok(Duration::from_secs_f64(secs))
}

pub fn list(volume: String, version: Option<String>) -> anyhow::Result<()> {
    init_logging(false, true, None)?;

    let runtime = new_runtime(None)?;

    let version = version.map(|v| v.parse()).transpose()?;
    let mut client = Client::new(volume)?;
    let volume = runtime.block_on(client.load_volume(&version))?;

    macro_rules! write_stdout {
        ($($args:tt)*) => {
            if let Err(_e) = write!(std::io::stdout(), $($args,)*) {
                return Ok(())
            }
        };
    }

    for entry in volume.walk(Ino::Root)? {
        let entry = entry?;

        // don't list/dump special files
        if !entry.is_regular() {
            continue;
        }

        match entry.attr().kind {
            FileType::Regular => {
                let path = entry.path();
                let (location, byte_range) = entry
                    .location()
                    .unwrap_or_else(|| panic!("BUG: missing location for regular file: {path}"));
                let offset = byte_range.offset;
                let len = byte_range.len;
                write_stdout!("{len:>16} {path} -> {location} @ {offset}");
            }
            FileType::Directory => {
                let len = 0;
                let path = entry.path();
                write_stdout!("{len:>16} {path:40}");
            }
        }
    }

    Ok(())
}

pub fn create(
    dir: impl AsRef<Path>,
    volume: impl AsRef<str>,
    version: impl AsRef<str>,
) -> anyhow::Result<()> {
    init_logging(false, true, None)?;

    let runtime = new_runtime(None)?;
    let mut client = Client::new(volume.as_ref())?;
    let version = Version::from_str(version.as_ref())?;

    runtime.block_on(async {
        let mut volume = client.create_volume().await;
        volume.pack(dir, version).await?;
        Ok(())
    })
}

pub fn versions(volume: String) -> anyhow::Result<()> {
    init_logging(false, true, None)?;

    let runtime = new_runtime(None)?;
    let client = Client::new(&volume)?;
    let versions = runtime.block_on(client.list_versions())?;

    if versions.is_empty() {
        write_stderr!("No versions found under '{volume}'. Is this actually a volume?");
    } else {
        for version in versions {
            let _ = writeln!(std::io::stdout(), "{version}");
        }
    }

    Ok(())
}

pub fn mount(args: MountArgs) -> anyhow::Result<()> {
    if args.background {
        // mount in the background by forking and mounting in the child. the
        // parent hangs out just long to reap the child and exit nonzero if the
        // mount fails, but does nothing else.
        //
        // safety: do as little as possible before fork. UNIX sucks, so this is the
        // best we can do. the biggest danger here is adding something to `args`
        // that's not safe to fork with.
        //
        // we don't do the double-fork setsid trick here because we don't really
        // expect Pond to be used as a true daemon - you're probably starting it
        // in the background as part of a terminal session for convenience.
        let (read_fd, write_fd) = nix::unistd::pipe().expect("failed to create a pipe");
        match unsafe { nix::unistd::fork().expect("failed to fork") } {
            // wait for a successful mount and then exit so we can exit with
            // a bad status if something goes wrong.
            ForkResult::Parent { child } => {
                std::mem::drop(write_fd);

                // mount timeout has to cover waiting to talk to S3 and fetching
                // the actual volume metadtata. it can't just be a handful of
                // millis while we wait for the mount syscall.
                let mount_timeout = Duration::from_secs(10);
                match read_pipe_status(read_fd, mount_timeout) {
                    // the child told us everything is good
                    Ok('0') => {
                        if !args.auto_unmount {
                            write_stderr!(
                                "WARN: auto_unmount is disabled. To unmount Pond, run: umount {mountpoint}",
                                mountpoint = args.mountpoint.display()
                            );
                        }
                        write_stderr!(
                            "{volume} is mounted at {mountpoint}",
                            volume = args.volume,
                            mountpoint = args.mountpoint.display()
                        );
                        Ok(())
                    }
                    // the child told us everything is bad
                    Ok(_) => {
                        if let Err(e) = waitpid(child, None) {
                            write_stderr!(
                                "error: failed waiting for child ({child}) process to exit: {e}"
                            )
                        }
                        Err(anyhow::anyhow!("mount failed"))
                    }
                    // something bad happened waiting for the child. assume it's hung
                    // and try to SIGTERM it.
                    Err(_) => {
                        if let Err(e) = nix::sys::signal::kill(child, Signal::SIGTERM) {
                            write_stderr!("error: failed to kill child process: {e}");
                        }
                        Err(anyhow::anyhow!(
                            "mount timed out after {t} seconds",
                            t = mount_timeout.as_secs()
                        ))
                    }
                }
            }
            // start the mount and report status back to
            // the parent.
            ForkResult::Child => {
                std::mem::drop(read_fd);
                let mut pipe = File::from(write_fd);

                let runtime = new_runtime(Some(&args))?;
                let metrics = new_metrics()?;
                let version = args.version.map(|v| Version::from_str(&v)).transpose()?;
                let client = Client::new(args.volume)?;
                let volume = runtime.block_on(
                    client
                        .with_metrics_snapshot_fn(Box::new(move || metrics.render().into_bytes()))
                        .with_cache_size(args.read_behavior.max_cache_size.as_u64())
                        .with_chunk_size(args.read_behavior.chunk_size.as_u64())
                        .with_readahead(args.read_behavior.readahead_size.as_u64())
                        .load_volume(&version),
                )?;

                let mut session = mount_volume(
                    runtime,
                    volume,
                    args.mountpoint,
                    args.allow_other,
                    args.auto_unmount,
                    args.kernel_cache_timeout,
                )?;

                // if the parent's been killed before we can write to it, something
                // is truly whacky and we should just bail. try to unmount and return.
                if pipe.write_all(b"0").is_err() {
                    session.unmount();
                    return Err(anyhow::anyhow!("failed to write mount status. exiting"));
                }

                // we're basically set, close all the standard fds and get ready
                // to run the mount session forever. at this point we're
                // assuming the mount will run successfully and any errors are
                // runtime errors, not setup errors.
                //
                // TODO: setup logging so that it logs to a local file or to a
                // special fuse file. until we have that covered, don't init tracing
                let _ = nix::unistd::close(std::io::stdin().as_raw_fd());
                let _ = nix::unistd::close(std::io::stdout().as_raw_fd());
                let _ = nix::unistd::close(std::io::stderr().as_raw_fd());
                init_logging(args.debug, false, args.log_file.as_deref())?;
                session.run()?;
                Ok(())
            }
        }
    } else {
        // mounting in the foreground.
        //
        // this looks almost exactly like what we run in the child but we don't
        // have to close FDs or communicate status. it's probably not worth
        // abstracting any more of this away than we already have.
        init_logging(args.debug, true, args.log_file.as_deref())?;

        let runtime = new_runtime(Some(&args))?;
        let metrics = new_metrics()?;
        let version = args.version.map(|v| Version::from_str(&v)).transpose()?;
        let client = Client::new(args.volume)?;
        let volume = runtime.block_on(
            client
                .with_metrics_snapshot_fn(Box::new(move || metrics.render().into_bytes()))
                .with_cache_size(args.read_behavior.max_cache_size.as_u64())
                .with_chunk_size(args.read_behavior.chunk_size.as_u64())
                .with_readahead(args.read_behavior.readahead_size.as_u64())
                .load_volume(&version),
        )?;

        let mut session = mount_volume(
            runtime,
            volume,
            args.mountpoint,
            args.allow_other,
            args.auto_unmount,
            args.kernel_cache_timeout,
        )?;
        session.run()?;
        Ok(())
    }
}

fn read_pipe_status(fd: OwnedFd, timeout: Duration) -> std::io::Result<char> {
    let mut buf = [0u8; 1];

    // open the pipe as a regular file. we're trusting that poll will only
    // return ready when there is actually data to read here, so we're not
    // doing the fcntl dance to set O_NONBLOCK
    let mut pipe = File::from(fd);

    let start = Instant::now();
    let mut poll_fds = [PollFd::new(pipe.as_fd(), PollFlags::POLLIN)];
    loop {
        let nready = nix::poll::poll(&mut poll_fds, timeout.as_millis() as u16)?;

        // only return once enough time has actually elapsed, poll can
        // wake up spuriously.
        if nready == 0 {
            if start.elapsed() > timeout {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out waiting for mount status",
                ));
            }
            continue;
        }

        let status_char = match pipe.read_exact(&mut buf) {
            Ok(()) => buf[0] as char,
            Err(_) => '1',
        };
        return Ok(status_char);
    }
}

pub fn mount_volume(
    runtime: tokio::runtime::Runtime,
    volume: Volume,
    mountpoint: impl AsRef<Path>,
    allow_other: bool,
    auto_unmount: bool,
    kernel_cache_timeout: Duration,
) -> anyhow::Result<fuser::Session<Pond>> {
    startup_log(&volume, &mountpoint, auto_unmount);

    let pond = Pond::new(runtime, volume, None, None, kernel_cache_timeout);
    let mut opts = vec![
        fuser::MountOption::FSName("pond".to_string()),
        fuser::MountOption::Subtype("pond".to_string()),
        fuser::MountOption::NoDev,
        fuser::MountOption::NoAtime,
    ];
    if allow_other {
        opts.push(fuser::MountOption::AllowOther);
    }
    if auto_unmount {
        if !allow_other {
            opts.push(fuser::MountOption::AllowRoot);
        }
        opts.push(fuser::MountOption::AutoUnmount);
    }

    Ok(fuser::Session::new(pond, mountpoint, &opts)?)
}

/// Log information about the volume.
#[tracing::instrument(name = "startup", level = "info", skip_all)]
fn startup_log(volume: &Volume, mountpoint: impl AsRef<Path>, auto_unmount: bool) {
    tracing::info!("Configured cache with: {}", volume.cache_config());
    tracing::info!(
        "Using object store client: {}",
        volume.object_store_description()
    );
    tracing::info!(version = %volume.version(), mountpoint=mountpoint.as_ref().to_str(), "Mounting volume:");
    tracing::info!(
        "Staged files will be written under: {}",
        volume.staged_file_temp_dir().path().to_string_lossy()
    );

    if !auto_unmount {
        tracing::warn!(
            "auto_unmount is disabled. To unmount Pond, run: umount {mountpoint}",
            mountpoint = mountpoint.as_ref().display()
        );
    }
}

fn new_runtime(args: Option<&MountArgs>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    if let Some(args) = args {
        builder.worker_threads(args.worker_threads as usize);
        builder.max_blocking_threads(args.blocking_threads as usize);
    }

    builder.enable_all().build()
}

fn new_metrics() -> anyhow::Result<PrometheusHandle> {
    let builder = PrometheusBuilder::new();
    Ok(builder
        .idle_timeout(MetricKindMask::ALL, None)
        .install_recorder()?)
}
