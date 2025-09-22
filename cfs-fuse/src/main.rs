use std::{path::PathBuf, time::Duration};

use cfs_client::MountedClient;
use cfs_core::volume::Volume;

use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(long)]
    allow_other: bool,

    #[clap(long, default_value_t = true)]
    auto_unmount: bool,

    /// The CFS volume to mount.
    volume: PathBuf,

    /// The directory to mount at. Must already exist.
    mountpoint: PathBuf,
}

fn main() {
    let args = Args::parse();
    let volume_bs = std::fs::read(&args.volume).unwrap();
    let volume = Volume::from_bytes(&volume_bs).unwrap();

    let volume = MountedClient::new(volume);

    let cfs = Cfs { volume };

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

    fuser::mount2(cfs, args.mountpoint, &opts).unwrap();
}

struct Cfs {
    volume: MountedClient,
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
        eprintln!("lookup({parent}, {name:?})", name = name.display());
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let attr = match self.volume.lookup(parent.into(), name) {
            Ok(Some(attr)) => attr,
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
        eprintln!("getattr({ino}, {fh})", fh = _fh.unwrap_or_default());
        let Ok(attr) = self.volume.getattr(ino) else {
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
        eprintln!("readdir({ino}, {_fh}, {offset})");
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
