mod client;
mod trace;

use cfs_core::{Ino, volume::Volume};
use clap::Parser;
use client::{AsyncFileReader, Client};
use std::{collections::HashMap, io::SeekFrom, path::PathBuf, pin::Pin, time::Duration};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Parser)]
struct Args {
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

    /// The bucket in object storage the volume lives in.
    #[clap(default_value = "junctionlabs")]
    bucket: String,
}

fn main() {
    let args = Args::parse();
    let volume_bs = std::fs::read(&args.volume).unwrap();
    let volume = Volume::from_bytes(&volume_bs).unwrap();

    let cfs = Cfs::new(volume, args.bucket).expect("volume should be mountable");

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
        fuser::mount2(trace::Trace::new(cfs), args.mountpoint, &opts).unwrap();
    } else {
        fuser::mount2(cfs, args.mountpoint, &opts).unwrap();
    }
}

struct Cfs {
    volume: Client,
    runtime: tokio::runtime::Runtime,
    next_fh: u64,
    fhs: HashMap<u64, Pin<Box<dyn AsyncFileReader>>>,
}

impl Cfs {
    fn new(volume: Volume, bucket: String) -> Result<Self, client::Error> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let volume = runtime.block_on(Client::mount(volume, bucket))?;

        Ok(Self {
            volume,
            runtime,
            next_fh: 1,
            fhs: Default::default(),
        })
    }

    fn create_fh(&mut self, reader: Pin<Box<dyn AsyncFileReader>>) -> u64 {
        // FIXME: we should check for available filehandles instead of
        // trying to just limit this with a counter or wrapper or whatever.
        let fh = self.next_fh;
        self.next_fh = self.next_fh.checked_add(1).expect("filehandle overflow");
        self.fhs.insert(fh, reader);
        fh
    }
}

impl fuser::Filesystem for Cfs {
    // TODO: set up uid/gid with init
    // TODO: set capabilities with init

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let attr = match self.volume.lookup(parent.into(), name) {
            Ok(Some(attr)) => attr,
            // nope
            Ok(None) => {
                reply.error(libc::ENOENT);
                return;
            }
            // TODO: this should return EISDIR/ENOTDIR when appropriate
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        reply.entry(&Duration::new(0, 0), &fuse_attr(attr), 0);
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let Ok(attr) = self.volume.getattr(ino.into()) else {
            reply.error(libc::ENOENT);
            return;
        };
        reply.attr(&Duration::new(0, 0), &fuse_attr(attr));
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let Ok(iter) = self.volume.readdir(ino.into()) else {
            reply.error(libc::ENOENT);
            return;
        };
        let Ok(offset) = offset.try_into() else {
            reply.error(libc::EINVAL);
            return;
        };

        for (i, (name, attr)) in iter.enumerate().skip(offset) {
            let is_full = reply.add(attr.ino.into(), (i + 1) as i64, fuse_kind(attr.kind), name);
            if is_full {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let Ok(reader) = self.runtime.block_on(self.volume.open(ino.into())) else {
            reply.error(libc::EIO);
            return;
        };
        let fh = self.create_fh(reader);

        let mut flags = 0;
        // all special filehandles should set FOPEN_DIRECT_IO to bypass page
        // cache both to keep values updating and to force the kernel to read
        // a file even if we report it as length zero.
        //
        // https://www.kernel.org/doc/html/next/filesystems/fuse/fuse-io.html
        if !Ino::from(ino).is_regular() {
            flags |= fuser::consts::FOPEN_DIRECT_IO;
        }

        reply.opened(fh, flags);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let Some(reader) = self.fhs.get_mut(&fh) else {
            reply.error(libc::EBADF);
            return;
        };

        // TODO: FUSE_READ specifies offset as uint64_t in current versions,
        // which makes it weird that it's an i64 here. cast it away for now.
        let offset = offset as u64;
        if offset > 0 {
            if let Err(_e) = self.runtime.block_on(reader.seek(SeekFrom::Start(offset))) {
                reply.error(libc::EIO);
                return;
            }
        }

        // TODO: re-use a scratch buffer instead of allocating here
        let mut buf = vec![0u8; size as usize];
        match self.runtime.block_on(reader.read(&mut buf)) {
            Ok(n) => reply.data(&buf[..n]),
            Err(_) => reply.error(libc::EIO),
        }
    }
}

fn fuse_attr(attr: &cfs_core::FileAttr) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: attr.ino.into(),
        size: attr.size,
        blocks: 0, // FIXME
        atime: attr.mtime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.ctime,
        kind: fuse_kind(attr.kind),
        perm: 0o644,
        nlink: 2,   // FIXME
        uid: 0,     // FIXME
        gid: 0,     // FIXME
        rdev: 0,    // ignored
        blksize: 0, // ignored
        flags: 0,   // ignored
    }
}

fn fuse_kind(kind: cfs_core::FileType) -> fuser::FileType {
    match kind {
        cfs_core::FileType::Regular => fuser::FileType::RegularFile,
        cfs_core::FileType::Directory => fuser::FileType::Directory,
        cfs_core::FileType::Symlink => fuser::FileType::Symlink,
    }
}
