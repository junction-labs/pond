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
pub struct Args {
    #[command(subcommand)]
    pub cmd: Cmd,

    /// Include the full error chain for CLI error output. For a fully detailed backtrace, set
    /// RUST_BACKTRACE=1.
    #[clap(long, default_value_t = false)]
    pub backtrace: bool,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Print version info.
    Version,

    /// *Experimental* - Dump the metadata for a Pond volume.
    Dump {
        /// An existing volume path.
        volume: String,

        /// The version of the volume. Defaults to the lexographically greatest version in the
        /// volume.
        #[clap(long)]
        version: Option<String>,
    },

    /// *Experimental* - Pack a local directory into a Pond volume.
    Pack {
        /// An existing directory to pack into a volume.
        dir: String,

        /// A directory path or S3 prefix to put the volume in.
        to: String,

        /// The version of the volume. Defaults to version 1, assuming this is a
        /// new volume.
        version: String,
    },

    /// List all versions in the volume.
    List { volume: String },

    /// Run a pond FUSE mount.
    Mount(MountArgs),
}

#[derive(Parser)]
pub struct MountArgs {
    #[clap(short, long, default_value_t = false)]
    pub debug: bool,

    #[clap(long)]
    pub allow_other: bool,

    #[clap(long, default_value_t = true)]
    pub auto_unmount: std::primitive::bool,

    pub volume: String,

    /// The directory to mount at. Must already exist.
    pub mountpoint: PathBuf,

    /// Specific version of the volume to mount. Defaults to the lexographically greatest version in
    /// the volume.
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

    /// The size of the chunk we fetch from object storage in a single request. It's also the size
    /// of the buffers we store within a single cache entry.
    #[clap(long, default_value = "8MiB", value_parser = value_parser!(ByteSize))]
    chunk_size: ByteSize,

    /// Size of readahead. If you're reading a file at byte 0, we will pre-fetch the bytes up to `readahead_size` in parallel.
    #[clap(long, default_value = "32MiB", value_parser = value_parser!(ByteSize))]
    readahead_size: ByteSize,
}

pub fn dump(
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

pub fn pack(
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

pub fn list(runtime: tokio::runtime::Runtime, volume: String) -> anyhow::Result<()> {
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
