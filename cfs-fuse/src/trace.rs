use fuser::Filesystem;

// TODO: this is a really simple wrapper over a filesystem that calls
// tracing::info! on every FUSE call. to really do the thing, we should write
// events in-memory and then make them available through a special file in the
// FUSE itself. the right way to do that is a ring-buffer or broadcast channel
// with open filehandles reading from it, instead of writing through tracing.

pub(crate) struct Trace<F> {
    inner: F,
}

impl<F> Trace<F> {
    pub fn new(fs: F) -> Self {
        Self { inner: fs }
    }
}

impl<F> Filesystem for Trace<F>
where
    F: Filesystem,
{
    fn init(
        &mut self,
        req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        tracing::info!("init()");
        self.inner.init(req, config)
    }

    fn destroy(&mut self) {
        tracing::info!("destroy()");
        self.inner.destroy();
    }

    fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        tracing::info!("lookup(parent: {parent:#x?}, name {name:?})");
        self.inner.lookup(req, parent, name, reply);
    }

    fn forget(&mut self, req: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        tracing::info!("forget(ino: {ino:#x?}, nlookup: {nlookup:#?})");
        self.inner.forget(req, ino, nlookup);
    }

    fn batch_forget(&mut self, req: &fuser::Request<'_>, nodes: &[fuser::fuse_forget_one]) {
        tracing::info!("batch_forget(nodes: {nodes:#x?})");
        self.inner.batch_forget(req, nodes);
    }

    fn getattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        tracing::info!(
            "getattr(ino: {ino:#x?}, fh: {fh:#x?})",
            fh = fh.unwrap_or_default()
        );
        self.inner.getattr(req, ino, fh, reply);
    }

    fn setattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        tracing::info!(
            "setattr(ino: {ino:#x?}, mode: {:#x?}, uid: {:#x?}, \
            gid: {:#x?}, size: {:#x?}, fh: {:#x?}, flags: {:#x?})",
            mode.unwrap_or_default(),
            uid.unwrap_or_default(),
            gid.unwrap_or_default(),
            size.unwrap_or_default(),
            fh.unwrap_or_default(),
            flags.unwrap_or_default()
        );
        self.inner.setattr(
            req, ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime,
            flags, reply,
        );
    }

    fn readlink(&mut self, req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        tracing::info!("readlink(ino: {ino:#x?})");
        self.inner.readlink(req, ino, reply);
    }

    fn mknod(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        tracing::info!(
            "mknod(parent: {parent:#x?}, name: {name:?}, \
            mode: {mode:#x?}, umask: {umask:#x?}, rdev: {rdev:#x?})"
        );
        self.inner
            .mknod(req, parent, name, mode, umask, rdev, reply);
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        tracing::info!(
            "mkdir(parent: {parent:#x?}, name: {name:?}, mode: {mode:#x?}, umask: {umask:#x?})"
        );
        self.inner.mkdir(req, parent, name, mode, umask, reply);
    }

    fn unlink(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("unlink(parent: {parent:#x?}, name: {name:?})");
        self.inner.unlink(req, parent, name, reply);
    }

    fn rmdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("rmdir(parent: {parent:#x?}, name: {name:?})");
        self.inner.rmdir(req, parent, name, reply);
    }

    fn symlink(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        link_name: &std::ffi::OsStr,
        target: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        tracing::info!(
            "symlink(parent: {parent:#x?}, link_name: {link_name:?}, target: {target:?})",
        );
        self.inner.symlink(req, parent, link_name, target, reply);
    }

    fn rename(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!(
            "rename(parent: {parent:#x?}, name: {name:?}, \
            newparent: {newparent:#x?}, newname: {newname:?}, flags: {flags:#x?})",
        );
        self.inner
            .rename(req, parent, name, newparent, newname, flags, reply);
    }

    fn link(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        tracing::info!("link(ino: {ino:#x?}, newparent: {newparent:#x?}, newname: {newname:?})");
        self.inner.link(req, ino, newparent, newname, reply);
    }

    fn open(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        tracing::info!("open(ino: {ino:#x?}, flags: {flags:#x?})");
        self.inner.open(req, ino, flags, reply);
    }

    fn read(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        tracing::info!(
            "read(ino: {ino:#x?}, fh: {fh:#x?}, offset: {offset:#x?}, \
            size: {size:#x?}, flags: {flags:#x?}, lock_owner: {:#x?})",
            lock_owner.unwrap_or_default()
        );
        self.inner
            .read(req, ino, fh, offset, size, flags, lock_owner, reply);
    }

    fn write(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        tracing::info!(
            "write(ino: {ino:#x?}, fh: {fh:#x?}, offset: {offset:#x?}, \
            data.len(): {:#x?}, write_flags: {write_flags:#x?}, flags: {flags:#x?}, \
            lock_owner: {:#x?})",
            data.len(),
            lock_owner.unwrap_or_default()
        );
        self.inner.write(
            req,
            ino,
            fh,
            offset,
            data,
            write_flags,
            flags,
            lock_owner,
            reply,
        );
    }

    fn flush(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("flush(ino: {ino:#x?}, fh: {fh:#x?}, lock_owner: {lock_owner:#x?})");
        self.inner.flush(req, ino, fh, lock_owner, reply);
    }

    fn release(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        lock_owner: Option<u64>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!(
            "release(ino: {ino:#x?}, fh: {fh:#x?}, flags: {flags:#x?}, lock_owner: {:#x?}, flush: {flush})",
            lock_owner.unwrap_or_default()
        );
        self.inner
            .release(req, ino, fh, flags, lock_owner, flush, reply);
    }

    fn fsync(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("fsync(ino: {ino:#x?}, fh: {fh:#x?}, datasync: {datasync})");
        self.inner.fsync(req, ino, fh, datasync, reply);
    }

    fn opendir(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        tracing::info!("opendir(ino: {ino:#x?}, flags: {flags:#x?})");
        self.inner.opendir(req, ino, flags, reply);
    }

    fn readdir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectory,
    ) {
        tracing::info!("readdir(ino: {ino:#x?}, fh: {fh:#x?}, offset: {offset:#x?})");
        self.inner.readdir(req, ino, fh, offset, reply);
    }

    fn readdirplus(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        tracing::info!("readdirplus(ino: {ino:#x?}, fh: {fh:#x?}, offset: {offset:#x?})");
        self.inner.readdirplus(req, ino, fh, offset, reply);
    }

    fn releasedir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("releasedir(ino: {ino:#x?}, fh: {fh:#x?}, flags: {flags:#x?})");
        self.inner.releasedir(req, ino, fh, flags, reply);
    }

    fn fsyncdir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("fsyncdir(ino: {ino:#x?}, fh: {fh:#x?}, datasync: {datasync})");
        self.inner.fsyncdir(req, ino, fh, datasync, reply);
    }

    fn statfs(&mut self, req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyStatfs) {
        tracing::info!("statfs(ino: {ino:#x?})");
        self.inner.statfs(req, ino, reply);
    }

    fn setxattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!(
            "setxattr(ino: {ino:#x?}, name: {name:?}, \
            flags: {flags:#x?}, position: {position})"
        );
        self.inner
            .setxattr(req, ino, name, value, flags, position, reply);
    }

    fn getxattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        tracing::info!("getxattr(ino: {ino:#x?}, name: {name:?}, size: {size:#x?})");
        self.inner.getxattr(req, ino, name, size, reply);
    }

    fn listxattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        tracing::info!("listxattr(ino: {ino:#x?}, size: {size:#x?})");
        self.inner.listxattr(req, ino, size, reply);
    }

    fn removexattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!("removexattr(ino: {ino:#x?}, name: {name:?})");
        self.inner.removexattr(req, ino, name, reply);
    }

    fn access(&mut self, req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        tracing::info!("access(ino: {ino:#x?}, mask: {mask:#x?})");
        self.inner.access(req, ino, mask, reply);
    }

    fn create(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        tracing::info!(
            "create(parent: {parent:#x?}, name: {name:?}, mode: {mode:#x?}, \
            umask: {umask:#x?}, flags: {flags:#x?})"
        );
        self.inner
            .create(req, parent, name, mode, umask, flags, reply);
    }

    fn getlk(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        tracing::info!(
            "getlk(ino: {ino:#x?}, fh: {fh:#x?}, lock_owner: {lock_owner:#x?}, \
            start: {start:#x?}, end: {end:#x?}, typ: {typ:#x?}, pid: {pid:#x?})"
        );
        self.inner
            .getlk(req, ino, fh, lock_owner, start, end, typ, pid, reply);
    }

    fn setlk(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!(
            "setlk(ino: {ino:#x?}, fh: {fh:#x?}, lock_owner: {lock_owner:#x?}, \
            start: {start:#x?}, end: {end:#x?}, typ: {typ:#x?}, pid: {pid:#x?}, sleep: {sleep})"
        );
        self.inner
            .setlk(req, ino, fh, lock_owner, start, end, typ, pid, sleep, reply);
    }

    fn bmap(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        blocksize: u32,
        idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        tracing::info!("bmap(ino: {ino:#x?}, blocksize: {blocksize:#x?}, idx: {idx:#x?})");
        self.inner.bmap(req, ino, blocksize, idx, reply);
    }

    fn ioctl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        tracing::info!(
            "ioctl(ino: {ino:#x?}, fh: {fh:#x?}, flags: {flags:#x?}, \
            cmd: {cmd:#x?}, in_data.len(): {:#x?}, out_size: {out_size:#x?})",
            in_data.len()
        );
        self.inner
            .ioctl(req, ino, fh, flags, cmd, in_data, out_size, reply);
    }

    fn poll(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        ph: fuser::PollHandle,
        events: u32,
        flags: u32,
        reply: fuser::ReplyPoll,
    ) {
        tracing::info!(
            "poll(ino: {ino:#x?}, fh: {fh:#x?}, \
            ph: {ph:?}, events: {events:#x?}, flags: {flags:#x?})"
        );
        self.inner.poll(req, ino, fh, ph, events, flags, reply);
    }

    fn fallocate(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        tracing::info!(
            "fallocate(ino: {ino:#x?}, fh: {fh:#x?}, \
            offset: {offset:#x?}, length: {length:#x?}, mode: {mode:#x?})"
        );
        self.inner
            .fallocate(req, ino, fh, offset, length, mode, reply);
    }

    fn lseek(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        tracing::info!(
            "lseek(ino: {ino:#x?}, fh: {fh:#x?}, \
            offset: {offset:#x?}, whence: {whence:#x?})"
        );
        self.inner.lseek(req, ino, fh, offset, whence, reply);
    }

    fn copy_file_range(
        &mut self,
        req: &fuser::Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        tracing::info!(
            "copy_file_range(ino_in: {ino_in:#x?}, fh_in: {fh_in:#x?}, \
            offset_in: {offset_in:#x?}, ino_out: {ino_out:#x?}, fh_out: {fh_out:#x?}, \
            offset_out: {offset_out:#x?}, len: {len:#x?}, flags: {flags:#x?})"
        );
        self.inner.copy_file_range(
            req, ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags, reply,
        );
    }
}
