mod fuse;

use bytesize::ByteSize;
use clap::{Parser, Subcommand, value_parser};
use pond::{Client, Error, ErrorKind, Version, Volume};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::fuse::Pond;

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
    #[clap(short, long, default_value_t = false)]
    pub debug: bool,

    /// All all other users of the system to access the filesystem.
    ///
    /// By convention, this is disabled by default in FUSE implementations. See
    /// the libfuse wiki for an explanation of why.
    ///
    /// https://github.com/libfuse/libfuse/wiki/FAQ#why-dont-other-users-have-access-to-the-mounted-filesystem
    #[clap(long)]
    pub allow_other: bool,

    /// Automatically unmount the filesystem if this process exits for any
    /// reason.
    #[clap(long, default_value_t = true)]
    pub auto_unmount: std::primitive::bool,

    /// The URL of the volume to mount.
    pub volume: String,

    /// The local directory to mount the volume at. Must already exist.
    pub mountpoint: PathBuf,

    /// The version of the volume to mount. Defaults to the lexographically
    /// greatest version in the volume if not specified.
    #[clap(long)]
    pub version: Option<String>,

    #[command(flatten)]
    pub read_behavior: ReadBehaviorArgs,
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

pub fn list(
    runtime: tokio::runtime::Runtime,
    volume: String,
    version: Option<String>,
) -> anyhow::Result<()> {
    let version = version.map(|v| v.parse()).transpose()?;
    let client = Client::new(volume)?;
    let volume = runtime.block_on(client.load_volume(&version))?;
    volume.dump()?;
    Ok(())
}

pub fn create(
    runtime: tokio::runtime::Runtime,
    dir: impl AsRef<Path>,
    to: impl AsRef<str>,
    version: impl AsRef<str>,
) -> anyhow::Result<()> {
    let client = Client::new(to.as_ref())?;
    let version = Version::from_str(version.as_ref())?;

    runtime.block_on(async {
        let mut volume = client.create_volume().await;
        volume.pack(dir, version).await?;
        Ok(())
    })
}

pub fn versions(runtime: tokio::runtime::Runtime, volume: String) -> anyhow::Result<()> {
    let client = Client::new(volume)?;
    let versions = runtime.block_on(client.list_versions())?;

    if versions.is_empty() {
        return Err(Error::new(ErrorKind::NotFound, "empty volume").into());
    }

    for version in versions {
        println!("{version}");
    }
    Ok(())
}

pub fn mount(runtime: tokio::runtime::Runtime, args: MountArgs) -> anyhow::Result<()> {
    let version = args.version.map(|v| Version::from_str(&v)).transpose()?;
    let client = Client::new(args.volume)?;
    let volume = runtime.block_on(
        client
            .with_cache_size(args.read_behavior.chunk_size.as_u64())
            .with_chunk_size(args.read_behavior.chunk_size.as_u64())
            .with_readahead(args.read_behavior.readahead_size.as_u64())
            .load_volume(&version),
    )?;

    let mut session = mount_volume(
        args.mountpoint,
        volume,
        runtime,
        args.allow_other,
        args.auto_unmount,
    )?;
    session.run()?;
    Ok(())
}

pub fn mount_volume(
    mountpoint: impl AsRef<Path>,
    volume: Volume,
    runtime: tokio::runtime::Runtime,
    allow_other: bool,
    auto_unmount: bool,
) -> anyhow::Result<fuser::Session<Pond>> {
    let pond = Pond::new(runtime, volume, None, None);
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
