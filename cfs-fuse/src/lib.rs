mod fuse;

use bytesize::ByteSize;
use cfs_core::{Client, Ino, Version};
use cfs_core::{Location, Volume};
use clap::{Parser, Subcommand, value_parser};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use crate::fuse::Cfs;

#[derive(Parser)]
pub struct Args {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// *Experimental* - Dump the metadata for a CFS volume.
    Dump {
        /// An existing volume path.
        volume: String,

        /// The version of the volume. Defaults to the latest version of the
        /// volume.
        #[clap(long)]
        version: Option<String>,
    },

    /// *Experimental* - Pack a local directory into a CFS volume.
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

    /// Run a cfs FUSE mount.
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

    /// Specific version of the volume to mount. Defaults to the latest version
    /// of the volume.
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
    fn location_path(l: &Location) -> String {
        match l {
            Location::Staged { .. } => format!("**{l}"),
            _ => format!("{l}"),
        }
    }

    let version = version.map(|v| v.parse()).transpose()?;
    let client = Client::new(volume)?;
    let volume = runtime.block_on(client.load_volume(&version))?;
    let metadata = volume.metadata();

    for (name, path, attrs) in metadata.walk(Ino::Root).unwrap() {
        if path.is_empty() && !attrs.ino.is_regular() {
            continue;
        }

        let kind;
        let location;
        let offset;
        let len;

        let path = {
            let mut full_path = path;
            full_path.push(name);
            full_path.join("/")
        };
        match attrs.kind {
            cfs_core::FileType::Regular => {
                kind = "f";
                let (l, b) = metadata.location(attrs.ino).unwrap();
                location = location_path(l);
                offset = b.offset;
                len = b.len;
            }
            cfs_core::FileType::Directory => {
                kind = "d";
                location = "".to_string();
                offset = 0;
                len = 0;
            }
        }
        println!("{kind:4} {location:40} {offset:12} {len:8} {path:40}");
    }

    Ok(())
}

pub fn pack(
    runtime: tokio::runtime::Runtime,
    dir: impl AsRef<Path>,
    to: impl AsRef<str>,
    version: impl AsRef<str>,
) -> anyhow::Result<()> {
    let client = Client::new(to.as_ref())?;
    // default to version 1
    let version = Version::from_str(version.as_ref())?;

    if runtime.block_on(client.exists(&version))? {
        anyhow::bail!("version {version} already exists");
    }
    let mut volume = runtime.block_on(client.create_volume())?;
    let metadata = volume.metadata_mut();

    // walk the entire tree in dfs order. make sure directories are sorted by
    // filename so that doing things like cat some/dir/* will traverse the
    // directory in the order we've packed it.
    let walk_root: &Path = dir.as_ref();
    let walker = walkdir::WalkDir::new(walk_root)
        .min_depth(1)
        .sort_by_file_name();

    for entry in walker {
        let entry = entry?;
        let path = entry.path().strip_prefix(walk_root).unwrap();

        // for a directory, just mkdir_all on the volume
        if entry.file_type().is_dir() {
            let dirs: Vec<_> = path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect();
            metadata.mkdir_all(Ino::Root, dirs)?;
        }
        // for a file:
        //
        // - write the content into the blob as bytes
        // - try to open the file (right now with mkdir_all, but it should maybe
        //   be lookup_all if we know this is a dfs?)
        // - write the file into the volume
        //
        // error handling here is interesting: how do we deal with a failure
        // writing the blob? how do we deal with a failure updating the volume?
        // both seem like they're unrecoverable.
        if entry.file_type().is_file() {
            let name = entry.file_name();
            let dir = path.ancestors().nth(1).unwrap();
            let dir_ino = if !dir.to_string_lossy().is_empty() {
                let dirs = dir
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string());
                metadata.mkdir_all(Ino::Root, dirs).unwrap().ino
            } else {
                Ino::Root
            };

            let len = entry.metadata().unwrap().len();
            metadata.create(
                dir_ino,
                name.to_string_lossy().to_string(),
                true,
                Location::staged(entry.path()),
                cfs_core::ByteRange { offset: 0, len },
            )?;
        }
    }

    runtime.block_on(volume.commit(version))?;

    Ok(())
}

pub fn list(runtime: tokio::runtime::Runtime, volume: String) -> anyhow::Result<()> {
    let client = Client::new(volume)?;
    let versions = runtime.block_on(client.list_versions())?;
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
) -> anyhow::Result<fuser::Session<Cfs>> {
    let cfs = Cfs::new(runtime, volume);
    let mut opts = vec![
        fuser::MountOption::FSName("cfs".to_string()),
        fuser::MountOption::Subtype("cool".to_string()),
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

    Ok(fuser::Session::new(cfs, mountpoint, &opts)?)
}
