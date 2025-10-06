use cfs_core::{ByteRange, Volume};
use cfs_core::{Ino, VolumeMetadata};
use std::time::Duration;
use std::time::SystemTime;

// TODO: write a conversion from VolumeERror to libc error codes
// TODO: write a try! macro that gets rid of most of the let-else spam in here.

pub(crate) struct Cfs {
    volume: Volume,
    runtime: tokio::runtime::Runtime,
}

impl Cfs {
    pub(crate) fn new(
        runtime: tokio::runtime::Runtime,
        metadata: VolumeMetadata,
        max_cache_size: u64,
        chunk_size: u64,
        readahead_size: u64,
    ) -> Self {
        let volume = Volume::new(metadata, max_cache_size, chunk_size, readahead_size);
        Self { volume, runtime }
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

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };

        match self.volume.mkdir(parent.into(), name.to_string()) {
            Ok(attr) => reply.entry(&Duration::new(0, 0), &fuse_attr(attr), 0),
            Err(_err) => {
                // FIXME: translate volume error to error code
                reply.error(libc::EINVAL);
            }
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.volume.rmdir(parent.into(), name) {
            Ok(()) => reply.ok(),
            // FIXME: translate volume error to error code
            Err(_e) => reply.error(libc::ENOENT),
        }
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        // O_RDONLY | O_TRUNC is undefined behavior in posix and we're
        // implicitly calling O_TRUNC on every create here, so we should be free
        // to just EINVAL readonly.
        match oflags_read_write(flags) {
            Some(OpenMode::Write | OpenMode::ReadWrite) => (),
            Some(OpenMode::Read) | None => {
                reply.error(libc::EINVAL);
                return;
            }
        }

        let excl = (flags & libc::O_EXCL) > 0;
        let parent: Ino = parent.into();
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };

        let Ok((attr, fd)) = self.volume.create(parent, name.to_string(), excl) else {
            reply.error(libc::EIO); // FIXME
            return;
        };
        reply.created(&Duration::new(0, 0), &fuse_attr(attr), 0, fd.into(), 0);
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let ino: Ino = ino.into();

        // all special filehandles should set FOPEN_DIRECT_IO to bypass page
        // cache both to keep values updating and to force the kernel to read
        // a file even if we report it as length zero.
        //
        // https://www.kernel.org/doc/html/next/filesystems/fuse/fuse-io.html
        let mut reply_flags = 0;
        if !ino.is_regular() {
            reply_flags |= fuser::consts::FOPEN_DIRECT_IO;
        }

        match oflags_read_write(flags) {
            // to open a file for reading, we ask the volume for an (async)
            // reader and stash it in the filehande set for later.
            Some(OpenMode::Read) => {
                let Ok(fd) = self.runtime.block_on(self.volume.open_read(ino)) else {
                    reply.error(libc::EIO);
                    return;
                };
                reply.opened(fd.into(), reply_flags);
            }
            // to open a file for writing, we modify the volume asap to try and
            // overwrite any existing files, and then stash a filehandle to a
            // local tempfile in the filehandle set for later.
            Some(OpenMode::Write | OpenMode::ReadWrite) => {
                let Ok(fd) = self.runtime.block_on(self.volume.open_read_write(ino)) else {
                    reply.error(libc::EIO);
                    return;
                };
                reply.opened(fd.into(), reply_flags);
            }
            // you set an illegal file mode
            None => reply.error(libc::EINVAL),
        }
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
        // TODO: re-use a scratch buffer instead of allocating here
        let mut buf = vec![0u8; size as usize];
        let res = self
            .runtime
            .block_on(self.volume.read_at(fh.into(), offset as u64, &mut buf));

        match res {
            Ok(n) => reply.data(&buf[..n]),
            Err(_) => reply.error(libc::EIO), // FIXME: convert error code
        }
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let res = self
            .runtime
            .block_on(self.volume.write_at(fh.into(), offset as u64, data));

        match res {
            Ok(n) => reply.written(n as u32),
            Err(_err) => reply.error(libc::EIO),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let ino = ino.into();
        if mtime.is_some() || ctime.is_some() {
            let mtime = mtime.map(|t| match t {
                fuser::TimeOrNow::SpecificTime(t) => t,
                fuser::TimeOrNow::Now => SystemTime::now(),
            });
            if let Err(_err) = self.volume.setattr(ino, mtime, ctime) {
                reply.error(libc::ENOENT);
                return;
            };
        };

        if let Some(size) = size
            && let Err(_err) = self.volume.modify(
                ino,
                None,
                Some(ByteRange {
                    offset: 0,
                    len: size,
                }),
            )
        {
            reply.error(libc::EINVAL);
            return;
        };

        let Ok(attr) = self.volume.getattr(ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        reply.attr(&Duration::new(0, 0), &fuse_attr(attr));
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Err(_err) = self.runtime.block_on(self.volume.release(fh.into())) {
            reply.error(libc::EINVAL);
        } else {
            reply.ok();
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };

        match self.volume.delete(parent.into(), name) {
            Err(_todo) => {
                // FIXME: actually insepct the error and pick a code
                reply.error(libc::ENOENT);
            }
            Ok(_) => {
                reply.ok();
            }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenMode {
    Read,
    Write,
    ReadWrite,
}

/// Read open flags and return a pair of booleans indicating whether the
/// file is being opened for read or write. Checks that the flags are specified
/// in a well-defined way.
///
/// From `man(2)` open:
///
/// > Unlike the other values that can be specified in flags, the access mode
/// > values O_RDONLY, O_WRONLY, and O_RDWR do not specify individual bits. Rather,
/// > they define the low order two bits of flags, and are defined respectively as
/// > 0, 1, and 2. In other words, the combination O_RDONLY | O_WRONLY is a logical
/// > error, and certainly does not have the same meaning as O_RDWR.
fn oflags_read_write(flags: i32) -> Option<OpenMode> {
    match flags & libc::O_ACCMODE /* 0x3 */ {
        libc::O_RDONLY => Some(OpenMode::Read),
        libc::O_WRONLY => Some(OpenMode::Write),
        libc::O_RDWR => Some(OpenMode::ReadWrite),
        _ => None,
    }
}
