mod fuse;
mod trace;

use bytesize::ByteSize;
use cfs_core::Location;
use cfs_core::{Ino, volume::Volume};
use clap::{Parser, Subcommand, value_parser};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use crate::fuse::Cfs;

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
        dir: String,
        volume: String,
        data: String,
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

    /// The CFS volume to mount.
    volume: PathBuf,

    /// The directory to mount at. Must already exist.
    mountpoint: PathBuf,

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
        Cmd::Pack { dir, volume, data } => pack(dir, volume, data),
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
            Location::Staged(i) => format!("staged({i})"),
            Location::Local { path, .. } => format!("{path}", path = path.display()),
            Location::ObjectStorage { bucket, key, .. } => format!("s3://{bucket}/{key}"),
        }
    }

    let bs = std::fs::read(volume.as_ref()).unwrap();
    let volume = Volume::from_bytes(&bs).unwrap();

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
        println!("{kind:4} {location:16} {offset:12} {len:8} {path:40}");
    }

    Ok(())
}

fn pack(
    dir: impl AsRef<Path>,
    volume: impl AsRef<Path>,
    data: impl AsRef<Path>,
) -> anyhow::Result<()> {
    use cfs_core::volume::Volume;

    let data = data.as_ref().to_path_buf();
    let root: &Path = dir.as_ref();
    let mut data_f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&data)?;
    let mut volume_f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(volume)?;

    let mut volume = Volume::empty();
    let mut cursor = 0u64;

    // create a staging blob
    let staging = Location::Staged(0);

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
            let n = std::io::copy(&mut file, &mut data_f).unwrap();
            // eprintln!("adding {path} to blob", path = entry.path().display());
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

    volume.relocate(
        &staging,
        Location::Local {
            path: data,
            len: cursor as usize,
        },
    )?;

    volume_f.write_all(&volume.to_bytes()?)?;

    Ok(())
}

fn mount(args: MountArgs) -> anyhow::Result<()> {
    let volume_bs = std::fs::read(&args.volume).unwrap();
    let volume = Volume::from_bytes(&volume_bs).unwrap();

    let cfs = Cfs::new(
        volume,
        args.chunk_size.as_u64(),
        args.readahead_size.as_u64(),
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
        tracing_subscriber::fmt::init();
        fuser::mount2(trace::Trace::new(cfs), args.mountpoint, &opts)?;
    } else {
        fuser::mount2(cfs, args.mountpoint, &opts)?;
    }

    Ok(())
}
