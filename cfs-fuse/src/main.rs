mod fuse;
mod trace;

use anyhow::Context;
use bytes::BytesMut;
use bytesize::ByteSize;
use cfs_core::Location;
use cfs_core::{Ino, VolumeMetadata};
use clap::{Parser, Subcommand, value_parser};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{ObjectStore, PutPayload};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::fuse::Cfs;

static VOL_FILENAME: &str = "vol.cfs.bin";
static DATA_FILENAME: &str = "data.cfs.bin";
static VERSION_SUBDIR: &str = "version";

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// *Experimental* - Dump the metadata for a CFS volume.
    Dump { volume: String },

    /// *Experimental* - Pack a local directory into a CFS volume.
    Pack {
        /// An existing directory to pack into a volume.
        dir: String,

        /// A directory path or S3 prefix to put the volume in.
        to: String,
    },

    /// List all versions in the volume.
    List {
        #[command(flatten)]
        volume: VolumeArgs,
    },

    /// Run a cfs FUSE mount.
    Mount(MountArgs),
}

#[derive(Parser)]
struct MountArgs {
    #[clap(short, long, default_value_t = false)]
    debug: bool,

    #[clap(long)]
    allow_other: bool,

    #[clap(long, default_value_t = true)]
    auto_unmount: bool,

    #[command(flatten)]
    volume: VolumeArgs,

    /// The directory to mount at. Must already exist.
    mountpoint: PathBuf,

    /// Specific version of the volume to mount. Default: latest version.
    #[clap(long)]
    version: Option<String>,

    #[command(flatten)]
    read_behavior: ReadBehaviorArgs,
}

#[derive(clap::Args, Debug)]
struct VolumeArgs {
    /// The path to the volume. Can be a local path or an S3 path.
    path: String,

    /// S3 region (only applicable when the volume path is an S3 path).
    #[arg(long, default_value = "us-east-2")]
    region: String,
}

#[derive(clap::Args, Debug)]
struct ReadBehaviorArgs {
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

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let res = match args.cmd {
        Cmd::Dump { volume } => dump(volume),
        Cmd::Pack { dir, to } => pack(dir, to),
        Cmd::List { volume } => list(volume),
        Cmd::Mount(mount_args) => mount(mount_args),
    };

    if let Err(e) = res {
        eprintln!("{e}");
        std::process::exit(-1);
    }
}

fn dump(volume: impl AsRef<Path>) -> anyhow::Result<()> {
    fn location_path(l: &Location) -> String {
        match l {
            Location::Staged { path } => format!("staged({path})", path = path.display()),
            Location::Local { path, .. } => format!("{path}", path = path.display()),
            Location::ObjectStorage { bucket, key, .. } => format!("s3://{bucket}/{key}"),
        }
    }

    let bs = std::fs::read(volume.as_ref()).unwrap();
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

fn pack(dir: impl AsRef<Path>, to: String) -> anyhow::Result<()> {
    enum PackLocation {
        Local {
            volume: PathBuf,
            data: PathBuf,
        },
        ObjectStorage {
            bucket: String,
            volume: String,
            data: String,
        },
    }

    let pack_location = match to.strip_prefix("s3://") {
        Some(bucket_and_key) => {
            // build some object storage keys/buckets. just upload no matter what.
            let (bucket, key) = bucket_and_key
                .split_once("/")
                .ok_or(anyhow::anyhow!("invalid s3 prefix"))?;
            let vol_key = format!("{key}/{VOL_FILENAME}", key = key.trim_end_matches("/"));
            let data_key = format!("{key}/{DATA_FILENAME}", key = key.trim_end_matches("/"));
            PackLocation::ObjectStorage {
                bucket: bucket.to_string(),
                volume: vol_key,
                data: data_key,
            }
        }
        None => {
            // check that the destination paths don't exist yet.
            if !std::fs::metadata(&to)?.is_dir() {
                anyhow::bail!("{to} is not a directory");
            }
            let volume = Path::new(&to).join(VOL_FILENAME);
            let data = Path::new(&to).join(DATA_FILENAME);

            if volume.exists() {
                anyhow::bail!("{volume}: already exists", volume = volume.display());
            }
            if data.exists() {
                anyhow::bail!("{data}: already exists", data = data.display())
            }
            PackLocation::Local { volume, data }
        }
    };

    let mut volume_file = tempfile::NamedTempFile::new()?;
    let mut data_file = tempfile::NamedTempFile::new()?;

    let root: &Path = dir.as_ref();
    let mut volume = VolumeMetadata::empty();
    let mut cursor = 0u64;

    // create a staging blob
    let staging = Location::Staged {
        path: data_file.path().to_path_buf(),
    };

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

    match &pack_location {
        PackLocation::Local {
            volume: volume_path,
            data: data_path,
        } => {
            volume.relocate(
                &staging,
                Location::Local {
                    path: data_path.to_path_buf(),
                },
            )?;

            // make the tempfiles permanent and relocate the blob
            let _ = data_file.persist(data_path)?;
            let mut volume_file = volume_file.persist(volume_path)?;
            volume_file.write_all(&volume.to_bytes()?)?;
        }
        PackLocation::ObjectStorage {
            bucket,
            volume: volume_key,
            data: data_key,
        } => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let s3 = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region("us-east-2")
                .build()?;

            volume.relocate(
                &staging,
                Location::ObjectStorage {
                    bucket: bucket.clone(),
                    key: data_key.clone(),
                },
            )?;

            volume_file.write_all(&volume.to_bytes()?)?;

            runtime.block_on(upload_file(&s3, volume_key, volume_file.path()))?;
            runtime.block_on(upload_file(&s3, data_key, data_file.path()))?;
        }
    };

    async fn upload_file(s3: &AmazonS3, key: &str, path: &Path) -> anyhow::Result<()> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut put = s3
            .put_multipart(&object_store::path::Path::from(key))
            .await?;

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

fn list(VolumeArgs { path, region }: VolumeArgs) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let url: url::Url = path.parse()?;
    let (object_store, path) = object_store::parse_url_opts(&url, [("region", region)])?;
    let path = version_dir(path);

    runtime.block_on(async {
        let result = object_store.list_with_delimiter(Some(&path)).await?;

        let mut table = comfy_table::Table::new();
        table.set_header(vec!["version", "last_modified"]);
        for path in result.common_prefixes {
            let Ok(metadata) = object_store.head(&path.child(VOL_FILENAME)).await else {
                continue;
            };
            table.add_row(vec![
                path.filename().unwrap_or("(empty)"),
                &metadata.last_modified.to_string(),
            ]);
        }

        if table.is_empty() {
            println!("(empty volume)");
        } else {
            println!("{table}");
        }

        Ok(())
    })
}

fn mount(args: MountArgs) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let volume = runtime.block_on(async {
        let url: url::Url = args.volume.path.parse()?;
        let (object_store, path) = object_store::parse_url_opts(&url, [("region", args.volume.region)])?;
        let path = version_dir(path);

        let path = if let Some(version) = args.version {
            // try to load the volume at this version for validation
            let path = path.child(version.clone()).child(VOL_FILENAME);
            object_store
                .head(&path)
                .await
                .context(format!("unable to load volume at version={version}"))?;
            path
        } else {
            // list all versions, and use the lexographically maximum version
            let result = object_store.list_with_delimiter(Some(&path)).await?;
            let path = result
                .common_prefixes
                .into_iter()
                .max()
                .ok_or(anyhow::anyhow!("could not find a volume version to mount. consider explicitly specifying --version"))?;
            path.child(VOL_FILENAME)
        };

        let bytes = object_store.get(&path).await?.bytes().await?;
        VolumeMetadata::from_bytes(&bytes).context("failed to parse Volume")
    })?;

    let cfs = Cfs::new(
        runtime,
        volume,
        args.read_behavior.max_cache_size.as_u64(),
        args.read_behavior.chunk_size.as_u64(),
        args.read_behavior.readahead_size.as_u64(),
    );
    let mut opts = vec![
        fuser::MountOption::FSName("cfs".to_string()),
        fuser::MountOption::Subtype("cool".to_string()),
        fuser::MountOption::NoDev,
        fuser::MountOption::NoAtime,
    ];
    if args.allow_other {
        opts.push(fuser::MountOption::AllowOther);
    }
    if args.auto_unmount {
        opts.extend([
            fuser::MountOption::AllowRoot,
            fuser::MountOption::AutoUnmount,
        ]);
    }

    if args.debug {
        fuser::mount2(trace::Trace::new(cfs), args.mountpoint, &opts)?;
    } else {
        fuser::mount2(cfs, args.mountpoint, &opts)?;
    }

    Ok(())
}

fn version_dir(path: object_store::path::Path) -> object_store::path::Path {
    path.child(VERSION_SUBDIR)
}
