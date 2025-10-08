mod fuse;
mod trace;

use anyhow::Context;
use bytes::BytesMut;
use bytesize::ByteSize;
use cfs_core::{Ino, VolumeError, VolumeMetadata};
use cfs_core::{Location, Volume};
use clap::{Parser, Subcommand, value_parser};
use object_store::{ObjectStore, PutPayload};
use std::io::Write;
use std::num::NonZeroU64;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncReadExt};

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
        version: Option<NonZeroU64>,
    },

    /// *Experimental* - Pack a local directory into a CFS volume.
    Pack {
        /// An existing directory to pack into a volume.
        dir: String,

        /// A directory path or S3 prefix to put the volume in.
        to: String,

        /// The version of the volume. Defaults to version 1, assuming this is a
        /// new volume.
        #[clap(long)]
        version: Option<NonZeroU64>,
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
    pub version: Option<NonZeroU64>,

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
    version: Option<NonZeroU64>,
) -> anyhow::Result<()> {
    fn location_path(l: &Location) -> String {
        match l {
            Location::Staged { .. } => format!("**{l}"),
            _ => format!("{l}"),
        }
    }

    let volume = Location::from_str(&volume)?;
    let object_store = cfs_core::object_store::from_location(&volume)?;

    let version = match version {
        Some(v) => v.get(),
        None => {
            let versions = runtime.block_on(cfs_core::object_store::list_versions(
                &object_store,
                &volume,
            ))?;
            versions
                .iter()
                .cloned()
                .max()
                .ok_or_else(|| anyhow::anyhow!("no volume metadata found"))?
        }
    };
    let volume = volume.metadata(version);

    let bs = runtime.block_on(async {
        let res = object_store.get(&volume.path()).await?;
        res.bytes().await
    })?;
    let volume = VolumeMetadata::from_bytes(&bs).unwrap();

    for (name, path, attrs) in volume.walk(Ino::Root).unwrap() {
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
                let (l, b) = volume.location(attrs.ino).unwrap();
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
            cfs_core::FileType::Symlink => todo!(),
        }
        println!("{kind:4} {location:40} {offset:12} {len:8} {path:40}");
    }

    Ok(())
}

pub fn pack(
    runtime: tokio::runtime::Runtime,
    dir: impl AsRef<Path>,
    to: impl AsRef<str>,
    version: Option<NonZeroU64>,
) -> anyhow::Result<()> {
    // defaults to version 1
    let version = version.map(|v| v.get()).unwrap_or(1);

    let pack_location = Location::from_str(to.as_ref())?;
    let (volume_location, data_location) =
        (pack_location.metadata(version), pack_location.new_data());

    let object_store = cfs_core::object_store::from_location(&pack_location)?;

    runtime.block_on(async {
        if cfs_core::object_store::exists(&object_store, &volume_location).await? {
            anyhow::bail!("{volume_location}: already exists");
        }
        if cfs_core::object_store::exists(&object_store, &data_location).await? {
            anyhow::bail!("{data_location}: already exists");
        }
        Ok(())
    })?;

    let tempdir = tempfile::Builder::new()
        .prefix(".cfs-pack-")
        .tempdir_in(to.as_ref())?;

    let mut volume_file = tempfile::NamedTempFile::new_in(&tempdir)?;
    let mut data_file = tempfile::NamedTempFile::new_in(&tempdir)?;

    let root: &Path = dir.as_ref();
    let mut volume = VolumeMetadata::empty();
    let mut cursor = 0u64;

    // create a staging blob
    let staging = Location::staged(data_file.path().to_string_lossy().to_string());

    let walker = walkdir::WalkDir::new(root).min_depth(1);
    for entry in walker {
        let entry = entry?;
        let path = entry.path().strip_prefix(root).unwrap();

        // for a directory, just mkdir_all on the volume
        if entry.file_type().is_dir() {
            let dirs: Vec<_> = path
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect();
            volume.mkdir_all(Ino::Root, dirs)?;
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
                volume.mkdir_all(Ino::Root, dirs).unwrap().ino
            } else {
                Ino::Root
            };

            let mut file = std::fs::File::open(entry.path()).unwrap();
            let n = std::io::copy(&mut file, &mut data_file).unwrap();
            volume.create(
                dir_ino,
                name.to_string_lossy().to_string(),
                true,
                staging.clone(),
                cfs_core::ByteRange {
                    offset: cursor,
                    len: n,
                },
            )?;

            cursor += n;
        }
    }

    match volume.relocate(&staging, data_location.clone()) {
        Ok(()) => (),
        Err(VolumeError::DoesNotExist) if cursor == 0 => (), // we didn't do shit
        Err(e) => return Err(e.into()),
    };
    volume_file.write_all(&volume.to_bytes()?)?;

    match &pack_location {
        Location::Local { .. } => {
            data_file.persist(data_location.local_path().unwrap())?;
            volume_file.persist(volume_location.local_path().unwrap())?;
        }
        Location::ObjectStorage { .. } => {
            runtime.block_on(upload_file(
                &object_store,
                &data_location.path(),
                data_file.path(),
            ))?;
            runtime.block_on(upload_file(
                &object_store,
                &volume_location.path(),
                volume_file.path(),
            ))?;
        }
        _ => unreachable!("pack location should never be staged"),
    }

    Ok(())
}

async fn upload_file(
    s3: &dyn ObjectStore,
    key: &object_store::path::Path,
    local_path: &Path,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::open(local_path).await?;
    let mut put = s3.put_multipart(key).await?;

    let mut buf = BytesMut::with_capacity(1024 * 1024 * 100);
    loop {
        let n = read_up_to(&mut file, &mut buf, 1024 * 1024 * 100).await?;
        if n == 0 {
            break;
        }

        let bytes = buf.split_to(n);
        put.put_part(PutPayload::from_bytes(bytes.freeze())).await?;
        buf.reserve(1024 * 1024 * 100);
    }

    put.complete().await?;
    Ok(())
}

async fn read_up_to<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
    limit: usize,
) -> std::io::Result<usize> {
    buf.reserve(limit);
    let mut read = 0;

    loop {
        let n = r.read_buf(buf).await?;
        if n == 0 {
            break;
        }

        read += n;
        if read >= limit {
            break;
        }
    }

    Ok(read)
}

pub fn list(runtime: tokio::runtime::Runtime, location: String) -> anyhow::Result<()> {
    let location = Location::from_str(&location)?;
    let object_store = cfs_core::object_store::from_location(&location)?;

    let versions = runtime.block_on(cfs_core::object_store::list_versions(
        &object_store,
        &location,
    ))?;
    for version in versions {
        println!("{version}");
    }
    Ok(())
}

pub fn mount(args: MountArgs) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let location = Location::from_str(&args.volume)?;
    let object_store = cfs_core::object_store::from_location(&location)?;

    let version: u64 = match args.version {
        Some(v) => v.get(),
        None => {
            let versions = runtime.block_on(cfs_core::object_store::list_versions(
                &object_store,
                &location,
            ))?;
            versions
                .iter()
                .cloned()
                .max()
                .ok_or_else(|| anyhow::anyhow!("no volume metadata found"))?
        }
    };
    let location = location.metadata(version);

    let bytes = runtime.block_on(async {
        let res = object_store.get(&location.path()).await?;
        res.bytes().await
    })?;

    let metadata = VolumeMetadata::from_bytes(&bytes).context("failed to parse volume")?;
    let volume = Volume::new(
        location,
        metadata,
        args.read_behavior.max_cache_size.as_u64(),
        args.read_behavior.chunk_size.as_u64(),
        args.read_behavior.readahead_size.as_u64(),
        object_store,
    );

    let _session = mount_volume(
        args.mountpoint,
        volume,
        runtime,
        args.allow_other,
        args.auto_unmount,
        args.debug,
    )?;

    // TODO: wait for signals and try to gracefully shut down here. if you
    // run with auto-unmount fuser will do the thing here even though we're
    // not calling session.join() ourselves.
    loop {
        std::thread::park();
    }
}

pub fn mount_volume(
    mountpoint: impl AsRef<Path>,
    volume: Volume,
    runtime: tokio::runtime::Runtime,
    allow_other: bool,
    auto_unmount: bool,
    debug: bool,
) -> anyhow::Result<fuser::BackgroundSession> {
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

    let session = if debug {
        fuser::spawn_mount2(trace::Trace::new(cfs), mountpoint, &opts)?
    } else {
        fuser::spawn_mount2(cfs, mountpoint, &opts)?
    };
    Ok(session)
}
