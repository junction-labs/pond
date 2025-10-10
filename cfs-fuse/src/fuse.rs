use cfs_core::{ByteRange, ErrorKind, Fd, Volume};
use cfs_core::{Ino, Modify};
use std::ffi::OsStr;
use std::time::Duration;
use std::time::SystemTime;

// TODO: write a conversion from VolumeERror to libc error codes
// TODO: write a try! macro that gets rid of most of the let-else spam in here.

pub struct Cfs {
    volume: Volume,
    runtime: tokio::runtime::Runtime,
}

impl Cfs {
    pub(crate) fn new(runtime: tokio::runtime::Runtime, volume: Volume) -> Self {
        Self { volume, runtime }
    }
}

trait AsErrno {
    fn as_errno(&self) -> libc::c_int;
}

impl AsErrno for cfs_core::ErrorKind {
    fn as_errno(&self) -> libc::c_int {
        match self {
            cfs_core::ErrorKind::IsADirectory => libc::EISDIR,
            cfs_core::ErrorKind::NotADirectory => libc::ENOTDIR,
            cfs_core::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
            cfs_core::ErrorKind::AlreadyExists => libc::EEXIST,
            cfs_core::ErrorKind::NotFound => libc::ENOENT,
            cfs_core::ErrorKind::PermissionDenied => libc::EPERM,
            cfs_core::ErrorKind::InvalidData => libc::EINVAL,
            cfs_core::ErrorKind::TimedOut => libc::ETIMEDOUT,
            cfs_core::ErrorKind::Unsupported => libc::ENOTSUP,
            cfs_core::ErrorKind::Other | _ => libc::EIO,
        }
    }
}

macro_rules! fs_try {
    ($reply:expr, $e:expr $(,)*) => {
        match $e {
            Ok(v) => v,
            Err(err) => {
                let err: cfs_core::Error = err.into();
                $reply.error(err.kind().as_errno());
                return;
            }
        }
    };
}

fn from_os_str(s: &OsStr) -> cfs_core::Result<&str> {
    s.to_str().ok_or_else(|| ErrorKind::InvalidData.into())
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
        let name = fs_try!(reply, from_os_str(name));
        match fs_try!(reply, self.volume.lookup(parent.into(), name)) {
            Some(attr) => reply.entry(&Duration::new(0, 0), &fuse_attr(attr), 0),
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let attr = fs_try!(reply, self.volume.getattr(ino.into()));
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
        let iter = fs_try!(reply, self.volume.readdir(ino.into()));
        let offset = fs_try!(reply, offset.try_into().map_err(|_| ErrorKind::InvalidData));

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
        let name = fs_try!(reply, from_os_str(name));
        let attr = fs_try!(reply, self.volume.mkdir(parent.into(), name.to_string()));
        reply.entry(&Duration::new(0, 0), &fuse_attr(attr), 0);
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = fs_try!(reply, from_os_str(name));
        fs_try!(reply, self.volume.rmdir(parent.into(), name));
        reply.ok();
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

        let name = fs_try!(reply, from_os_str(name));
        let (attr, fd) = fs_try!(
            reply,
            self.volume.create(parent.into(), name.to_string(), excl)
        );
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
            Some(OpenMode::Read) => {
                let fd = fs_try!(reply, self.runtime.block_on(self.volume.open_read(ino)));
                reply.opened(fd.into(), reply_flags);
            }
            Some(OpenMode::Write | OpenMode::ReadWrite) => {
                let fd = fs_try!(
                    reply,
                    self.runtime.block_on(self.volume.open_read_write(ino))
                );
                reply.opened(fd.into(), reply_flags);
            }
            // you set an illegal file mode
            None => reply.error(libc::EINVAL),
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        // NOTE: it's not clear why offset is an i64 anywhere, libfuse defines offset as
        // a u64 in fuse_read_in in the kernel API. the high level libfuse api defines it
        // as an off_t which is a signed type.
        //
        // https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html#addd81057f639eec4b08927fc4c95dd41
        // https://github.com/libfuse/libfuse/blob/fuse-3.0.0/include/fuse_kernel.h#L517-L525
        let offset = fs_try!(reply, offset.try_into().map_err(|_| ErrorKind::InvalidData));

        let fd = Fd::new(ino.into(), fh);
        // TODO: re-use a scratch buffer instead of allocating here
        let mut buf = vec![0u8; size as usize];
        let n = fs_try!(
            reply,
            self.runtime
                .block_on(self.volume.read_at(fd, offset, &mut buf))
        );
        reply.data(&buf[..n]);
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let fd = Fd::new(ino.into(), fh);
        let n = fs_try!(
            reply,
            self.runtime
                .block_on(self.volume.write_at(fd, offset as u64, data))
        );

        reply.written(n as u32);
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
            fs_try!(reply, self.volume.setattr(ino, mtime, ctime));
        };

        if let Some(size) = size {
            fs_try!(
                reply,
                self.volume.modify(
                    ino,
                    None,
                    Some(Modify::Set(ByteRange {
                        offset: 0,
                        len: size,
                    })),
                )
            );
        };

        let attr = fs_try!(reply, self.volume.getattr(ino));
        reply.attr(&Duration::new(0, 0), &fuse_attr(attr));
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let fd = Fd::new(ino.into(), fh);
        fs_try!(reply, self.runtime.block_on(self.volume.release(fd)));
        reply.ok();
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = fs_try!(reply, from_os_str(name));
        fs_try!(reply, self.volume.delete(parent.into(), name));
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
