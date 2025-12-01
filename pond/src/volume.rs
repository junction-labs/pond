use crate::{
    ByteRange, DirEntry, Error, FileAttr, FileType, Ino, Location, Result,
    cache::{CacheConfig, ChunkCache},
    error::ErrorKind,
    metadata::{Modify, Version, VolumeMetadata},
    metrics::RecordLatencyGuard,
    scoped_timer,
};
use arc_swap::ArcSwap;
use backon::{ExponentialBuilder, Retryable};
use bytes::{Bytes, BytesMut};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::{
    collections::BTreeMap,
    io::{BufReader, Read},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

// TODO: We should use our own Path type abstraction here. std::fs::Path
// is pretty close to right but offers a bunch of system methods (like canonicalize)
// that mean it will do unexpected things if we use it as part of our interface.
//
// Both AWS and GCS commit to key names being valid utf8 but random filesystems we
// deal with won't enforce that. Since we're hiding object paths we have to figure
// out what we want to do about other encodings. Path/PathBuf are OsString under the
// hood which is right if we want to support that.

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fd {
    ino: Ino,
    fh: u64,
}

impl Fd {
    pub fn new(ino: Ino, fh: u64) -> Self {
        Self { ino, fh }
    }

    fn add_fh(&self, rhs: u64) -> Result<u64> {
        self.fh.checked_add(rhs).ok_or(Error::new(
            ErrorKind::Other,
            "fh value overflow, unable to read/write additional files",
        ))
    }
}

impl From<Fd> for u64 {
    fn from(fd: Fd) -> Self {
        fd.fh
    }
}

#[derive(Debug, Clone)]
enum FileDescriptor {
    Committed {
        key: Arc<object_store::path::Path>,
        range: ByteRange,
    },
    Staged {
        // Note: we could keep a bytes::Bytes around to re-use as a buffer when reading from and
        // writing to this file to avoid the overhead of allocations.
        file: Arc<std::fs::File>,
    },
    Version,
    Commit,
    ClearCache,
    PromMetrics {
        /// A static snapshot of the metrics at the time of FileDescriptor construction.
        snapshot: Arc<Vec<u8>>,
    },
}

// NOTE: When locking both meta and fds for modification, meta should always be locked first to
//       avoid deadlocks!
pub struct Volume {
    meta: Arc<RwLock<VolumeMetadata>>,
    cache: ChunkCache,
    fds: Arc<RwLock<BTreeMap<Fd, FileDescriptor>>>,
    store: crate::storage::Storage,
    metrics_snapshot: Option<Arc<ArcSwap<Vec<u8>>>>,
    // file generation number for staged files. the generation gives us an approximation of file
    // age relative to other files. the generation is bumped everytime we commit, so we're only at
    // risk of an overflow if the user commits 2^64 times throughout the lifetime of this process.
    // the generation number does not persist across multiple processes -- it is always reset back
    // to 0 since it's attached to staged (ephemeral) files.
    generation: AtomicU64,
    // is a commit in progress?
    commit: AtomicBool,
}

impl Volume {
    pub(crate) fn new(
        meta: VolumeMetadata,
        cache: ChunkCache,
        store: crate::storage::Storage,
        metrics_snapshot_fn: Option<Box<dyn Fn() -> Vec<u8> + Send>>,
    ) -> Self {
        let metrics_snapshot = metrics_snapshot_fn.map(|f| {
            let snapshot = Arc::new(ArcSwap::from_pointee(Vec::new()));
            tokio::spawn(metrics_refresh(f, cache.clone(), snapshot.clone()));
            snapshot
        });

        Self {
            meta: Arc::new(RwLock::new(meta)),
            cache,
            fds: Arc::new(RwLock::new(BTreeMap::new())),
            store,
            metrics_snapshot,
            generation: AtomicU64::new(0),
            commit: AtomicBool::new(false),
        }
    }

    pub(crate) fn modify(
        &self,
        ino: Ino,
        mtime: SystemTime,
        ctime: Option<SystemTime>,
        location: Option<Location>,
        range: Option<Modify>,
    ) -> Result<()> {
        match ino {
            Ino::CLEAR_CACHE | Ino::COMMIT => Ok(()),
            ino => {
                self.meta
                    .write()
                    .expect("lock was poisoned")
                    .modify(ino, mtime, ctime, location, range)?;
                Ok(())
            }
        }
    }

    pub fn version(&self) -> Version {
        self.meta
            .read()
            .expect("lock was poisoned")
            .version()
            .clone()
    }

    pub fn object_store_description(&self) -> String {
        self.store.object_store_description()
    }

    pub fn staged_file_temp_dir(&self) -> Arc<tempfile::TempDir> {
        self.store.staged_file_temp_dir()
    }

    pub fn cache_config(&self) -> &CacheConfig {
        self.cache.config()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.meta.read().expect("lock was poisoned").to_bytes()
    }

    pub fn to_bytes_with_version(&self, version: &Version) -> Result<Vec<u8>> {
        self.meta
            .read()
            .expect("lock was poisoned")
            .to_bytes_with_version(version)
    }

    pub fn getattr(&self, ino: Ino) -> Result<FileAttr> {
        scoped_timer!("pond_volume_getattr_latency_secs");
        match self.meta.read().expect("lock was poisoned").getattr(ino) {
            Some(attr) => Ok(attr.clone()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub fn setattr(
        &self,
        ino: Ino,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
    ) -> Result<FileAttr> {
        self.meta
            .write()
            .expect("lock was poisoned")
            .setattr(ino, mtime, ctime)
            .cloned()
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>> {
        scoped_timer!("pond_volume_lookup_latency_secs");
        Ok(self
            .meta
            .read()
            .expect("lock was poisoned")
            .lookup(parent, name)?
            .cloned())
    }

    pub fn mkdir(&self, parent: Ino, name: String) -> Result<FileAttr> {
        self.meta
            .write()
            .expect("lock was poisoned")
            .mkdir(parent, name)
            .cloned()
    }

    pub fn rmdir(&self, parent: Ino, name: &str) -> Result<()> {
        self.meta
            .write()
            .expect("lock was poisoned")
            .rmdir(parent, name)?;
        Ok(())
    }

    pub fn rename(&self, parent: Ino, name: &str, newparent: Ino, newname: String) -> Result<()> {
        self.meta
            .write()
            .expect("lock was poisoned")
            .rename(parent, name, newparent, newname)?;
        Ok(())
    }

    pub fn readdir(
        &self,
        ino: Ino,
        offset: Option<String>,
        size: usize,
    ) -> Result<impl Iterator<Item = DirEntry>> {
        let metadata = self.meta.read().expect("lock was poisoned");
        let entries: Vec<DirEntry> = metadata
            .readdir(ino, offset)?
            .take(size)
            .map(|e| e.to_owned())
            .collect();
        Ok(entries.into_iter())
    }

    pub async fn create(
        &self,
        parent: Ino,
        name: String,
        exclusive: bool,
    ) -> Result<(FileAttr, Fd)> {
        let path = self.store.new_staged_filepath()?;
        let file = open_file(path.as_path(), OpenMode::Create).await?;

        let attr = {
            let mut metadata = self.meta.write().expect("lock was poisoned");
            metadata
                .create(
                    parent,
                    name,
                    exclusive,
                    Location::staged(path, self.generation.load(Ordering::SeqCst)),
                    ByteRange::empty(),
                )?
                .clone()
            // we drop the write lock on metadata here -- the metadata and fd updates are
            // decoupled anyway, so right now a failed new_fd call doesn't cause us to undo the
            // metadata change. if we implemented transaction-like behavior, then it would be more
            // useful to hold both locks at the same time but as-is there's no reason to hold the
            // metadata for longer.
        };

        let fd = new_fd(
            &mut self.fds.write().expect("lock was poisoned"),
            attr.ino,
            FileDescriptor::Staged { file: file.into() },
        )?;
        Ok((attr, fd))
    }

    pub fn delete(&self, parent: Ino, name: &str) -> Result<()> {
        self.meta
            .write()
            .expect("lock was poisoned")
            .delete(parent, name)?;
        Ok(())
    }

    pub fn truncate(&self, ino: Ino, size: u64) -> Result<()> {
        self.modify(
            ino,
            SystemTime::now(),
            None,
            None,
            Some(Modify::Truncate(size)),
        )
    }
}

/// Periodically record global metrics and re-render PrometheusHandle metrics, writing
/// the result back into `shared` by replacing the internal Arc<String>.
async fn metrics_refresh(
    metrics_snapshot_fn: Box<dyn Fn() -> Vec<u8> + Send>,
    cache: ChunkCache,
    shared: Arc<ArcSwap<Vec<u8>>>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;

        // records current cache state (e.g. number of entries, size in bytes) by iterating and
        // locking each shards within the cache.
        cache.record_cache_size();

        shared.store(Arc::from(metrics_snapshot_fn()));
    }
}

impl Volume {
    /// Open a Fd to a locally staged file for reading and writing.
    ///
    /// Opening a Fd with write permissions will always truncate the file.
    #[allow(
        clippy::await_holding_lock,
        reason = "https://github.com/rust-lang/rust-clippy/issues/6446"
    )]
    pub async fn open_read_write(&self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::COMMIT => new_fd(
                &mut self.fds.write().expect("lock was poisoned"),
                ino,
                FileDescriptor::Commit,
            ),
            Ino::CLEAR_CACHE => new_fd(
                &mut self.fds.write().expect("lock was poisoned"),
                ino,
                FileDescriptor::ClearCache,
            ),
            Ino::VERSION => Err(ErrorKind::PermissionDenied.into()),
            ino => {
                let mut metadata_guard = self.meta.write().expect("lock was poisoned");
                match metadata_guard.location(ino) {
                    Some((Location::Staged { path, generation }, _)) => {
                        let file = if generation < self.generation.load(Ordering::SeqCst) {
                            // generation mismatch for staged files, which means we're opening this
                            // up while a commit is running and this staged file is part of the
                            // snapshot. we treat it as a committed file in this case.
                            let path = self.store.new_staged_filepath()?;
                            let staged = Location::staged(
                                path.clone(),
                                self.generation.load(Ordering::SeqCst),
                            );
                            metadata_guard.modify(
                                ino,
                                SystemTime::now(),
                                None,
                                Some(staged),
                                Some(Modify::Set((0, 0).into())),
                            )?;
                            std::mem::drop(metadata_guard); // guard is dropped here before the await
                            open_file(&path, OpenMode::Create).await?
                        } else {
                            let path = path.clone();
                            std::mem::drop(metadata_guard); // guard is dropped here before the await
                            open_file(&path, OpenMode::ReadWrite).await?
                        };

                        new_fd(
                            &mut self.fds.write().expect("lock was poisoned"),
                            ino,
                            FileDescriptor::Staged { file: file.into() },
                        )
                    }
                    Some((Location::Committed { .. }, ..)) => {
                        // truncate the file (by assigning it a brand new staged file) if it's
                        // committed. the alternative would be to keep a copy of the committed
                        // file locally as a staged file, which can be expensive if it's a large file.
                        let path = self.store.new_staged_filepath()?;
                        let staged =
                            Location::staged(path.clone(), self.generation.load(Ordering::SeqCst));
                        metadata_guard.modify(
                            ino,
                            SystemTime::now(),
                            None,
                            Some(staged),
                            Some(Modify::Set((0, 0).into())),
                        )?;
                        std::mem::drop(metadata_guard); // guard is dropped here before the await
                        let file = open_file(&path, OpenMode::Create).await?;
                        // only create the fd once the file is open and metadata is valid
                        new_fd(
                            &mut self.fds.write().expect("lock was poisoned"),
                            ino,
                            FileDescriptor::Staged { file: file.into() },
                        )
                    }
                    None => Err(ErrorKind::NotFound.into()),
                }
            }
        }
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "https://github.com/rust-lang/rust-clippy/issues/6446"
    )]
    pub async fn open_read(&self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::VERSION => new_fd(
                &mut self.fds.write().expect("lock was poisoned"),
                ino,
                FileDescriptor::Version,
            ),
            Ino::PROM_METRICS => {
                let data = match &self.metrics_snapshot {
                    Some(metrics) => metrics.load().clone(),
                    None => Default::default(),
                };
                new_fd(
                    &mut self.fds.write().expect("lock was poisoned"),
                    ino,
                    FileDescriptor::PromMetrics { snapshot: data },
                )
            }
            Ino::COMMIT | Ino::CLEAR_CACHE => Err(ErrorKind::PermissionDenied.into()),
            ino => {
                let metadata_guard = self.meta.read().expect("lock was poisoned");
                match metadata_guard.location(ino) {
                    Some((Location::Staged { path, .. }, _)) => {
                        let path = path.clone();
                        std::mem::drop(metadata_guard); // guard is dropped here before the await
                        let file = open_file(&path, OpenMode::Read).await?;
                        new_fd(
                            &mut self.fds.write().expect("lock was poisoned"),
                            ino,
                            FileDescriptor::Staged { file: file.into() },
                        )
                    }
                    Some((Location::Committed { key }, range)) => {
                        let key = Arc::new(self.store.child_path(key.as_ref()));
                        let range = *range;
                        std::mem::drop(metadata_guard);
                        new_fd(
                            &mut self.fds.write().expect("lock was poisoned"),
                            ino,
                            FileDescriptor::Committed { key, range },
                        )
                    }
                    None => Err(ErrorKind::NotFound.into()),
                }
            }
        }
    }

    pub async fn read_at(&self, fd: Fd, offset: u64, buf: &mut [u8]) -> Result<usize> {
        metrics::histogram!("pond_volume_read_buf_size_bytes").record(buf.len() as f64);

        let descriptor = self
            .fds
            .read()
            .expect("lock was poisoned")
            .get(&fd)
            .cloned();
        match descriptor {
            // reads of write-only special fds do nothing
            Some(FileDescriptor::ClearCache) | Some(FileDescriptor::Commit) => Ok(0),
            Some(FileDescriptor::Version) => {
                let version = self.version();
                read_version(&version, offset, buf)
            }
            Some(FileDescriptor::PromMetrics { snapshot }) => {
                read_from_buf(snapshot.as_ref(), offset, buf)
            }
            Some(FileDescriptor::Committed { key, range }) => {
                scoped_timer!("pond_volume_read_latency_secs", "type" => "committed");
                // FIXME: readahead needs to know the extent of the location -
                // the range here only includes the extent of THIS file in the
                // total blob. without knowing the full range we can TRY to prefetch
                // into the next chunk but we'll only get one at most - that banks
                // on the object store's API being kind enough to return partial ranges.
                let read_len = std::cmp::min(range.len.saturating_sub(offset), buf.len() as u64);
                if read_len == 0 {
                    return Ok(0);
                }
                let blob_offset = range.offset + offset;
                let bytes: Vec<Bytes> = self.cache.get_at(key, blob_offset, read_len).await?;
                Ok(copy_into(buf, &bytes))
            }
            Some(FileDescriptor::Staged { file, .. }) => {
                scoped_timer!("pond_volume_read_latency_secs", "type" => "staged");
                read_file_at(file.clone(), offset, buf).await
            }
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn write_at(&self, fd: Fd, offset: u64, data: &[u8]) -> Result<usize> {
        metrics::histogram!("pond_volume_write_buf_size_bytes").record(data.len() as f64);

        let descriptor = self
            .fds
            .read()
            .expect("lock was poisoned")
            .get(&fd)
            .cloned();
        match descriptor {
            Some(FileDescriptor::ClearCache) => {
                self.cache.clear();
                Ok(data.len())
            }
            Some(FileDescriptor::Commit) => {
                // only let writes happen at offset zero. man write(2) says
                // EINVAL is ok if "the file offset is not suitably aligned".
                if offset != 0 {
                    return Err(ErrorKind::InvalidData.into());
                }

                scoped_timer!("pond_volume_commit_latency_secs");

                // for writing to the magic fd - and only for writing to the
                // magic fd - we trim trailing ascii whitespace so that using
                // `echo` or `cat` to commit doesn't leave you with garbage
                // versions.
                //
                // even though we're trimming the bytes here, save the original
                // input length so that the caller doesn't try to write the
                // trailing space again.
                let data_len = data.len();
                let data = data.trim_ascii_end();

                let version = Version::from_bytes(data)?;
                self.commit(version).await?;
                Ok(data_len)
            }
            // write directly into a staged file
            Some(FileDescriptor::Staged { file, .. }) => {
                scoped_timer!("pond_volume_write_latency_secs");
                let n = write_file_at(file.clone(), offset, data).await?;
                self.modify(
                    fd.ino,
                    SystemTime::now(),
                    None,
                    None,
                    Some(Modify::Max(offset + n as u64)),
                )?;

                Ok(n)
            }
            // no other fds are writable
            Some(_) => Err(ErrorKind::PermissionDenied.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn release(&self, fd: Fd) -> Result<()> {
        match self.fds.write().expect("lock was poisoned").remove(&fd) {
            Some(_) => Ok(()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn commit(&self, version: Version) -> Result<()> {
        if self.store.exists(&version).await? {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("version {} already exists", &version),
            ));
        }

        let commit = Commit::new(self)?;

        // take a snapshot of the metadata and walk the volume to get a handle to each staged file.
        let mut snapshot = commit.snapshot()?;
        let (dest, range) = commit.upload_files(snapshot.staged_files).await?;
        apply_location_ranges(&mut snapshot.metadata, range, dest)?;
        commit
            .upload_metadata(&mut snapshot.metadata, version)
            .await?;

        // reconcile any differences between what we just uploaded and any modifications that
        // occurred since we took the snapshot.
        commit.sync(snapshot.metadata)
    }
}

fn read_version(version: &Version, offset: u64, buf: &mut [u8]) -> Result<usize> {
    read_from_buf(version.as_ref(), offset, buf)
}

fn read_from_buf(from: &[u8], offset: u64, to: &mut [u8]) -> Result<usize> {
    let offset: usize = offset.try_into().map_err(|_| ErrorKind::InvalidData)?;

    if offset > from.len() {
        return Ok(0);
    }

    let from = &from[offset..];
    let amt = std::cmp::min(to.len(), from.len());
    to[..amt].copy_from_slice(&from[..amt]);
    Ok(amt)
}

pub struct WalkVolume {
    meta: Arc<RwLock<VolumeMetadata>>,
    stack: Vec<std::vec::IntoIter<DirEntry>>,
}

impl WalkVolume {
    fn new(meta: Arc<RwLock<VolumeMetadata>>, ino: Ino) -> Result<Self> {
        let entries = meta
            .read()
            .expect("lock was poisoned")
            .readdir(ino, None)?
            .map(|e| e.to_owned())
            .collect::<Vec<_>>();
        Ok(Self {
            meta,
            stack: vec![entries.into_iter()],
        })
    }

    fn push_dir_entries(&mut self, ino: Ino) -> Result<()> {
        let entries = self
            .meta
            .read()
            .expect("lock was poisoned")
            .readdir(ino, None)?
            .map(|e| e.to_owned())
            .collect::<Vec<_>>();
        self.stack.push(entries.into_iter());
        Ok(())
    }
}

impl Iterator for WalkVolume {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let iter = self.stack.last_mut()?;
            if let Some(entry) = iter.next() {
                if entry.attr().kind == FileType::Directory
                    && let Err(e) = self.push_dir_entries(entry.attr().ino)
                {
                    return Some(Err(e));
                }
                return Some(Ok(entry));
            } else {
                self.stack.pop();
            }
        }
    }
}

impl Volume {
    /// Returns a guarded iterator over the entire volume. The guard ensures that iteration is done
    /// over a consistent view of the Volume.
    pub fn walk(&self, ino: Ino) -> Result<WalkVolume> {
        WalkVolume::new(self.meta.clone(), ino)
    }

    /// Pack a local directory into a Pond volume.
    pub async fn pack(&mut self, dir: impl AsRef<Path>, version: Version) -> crate::Result<()> {
        if self.store.exists(&version).await? {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("version {} already exists", &version),
            ));
        }
        // walk the entire tree in dfs order. make sure directories are sorted by
        // filename so that doing things like cat some/dir/* will traverse the
        // directory in the order we've packed it.
        let walk_root: &Path = dir.as_ref();
        let walker = walkdir::WalkDir::new(walk_root)
            .min_depth(1)
            .sort_by_file_name();

        for entry in walker {
            let entry = entry
                .map_err(|e| Error::with_source(ErrorKind::InvalidData, "failed to walk dir", e))?;
            let path = entry
                .path()
                .strip_prefix(walk_root)
                .map_err(|e| Error::with_source(ErrorKind::InvalidData, "prefix not found", e))?;

            // for a directory, just mkdir_all on the volume
            if entry.file_type().is_dir() {
                let dirs: Vec<_> = path
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string())
                    .collect();
                self.meta
                    .write()
                    .expect("lock was poisoned")
                    .mkdir_all(Ino::Root, dirs)?;
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
                let dir = path.parent().ok_or(Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to find parent of {}", path.to_string_lossy()),
                ))?;
                let dir_ino = if !dir.to_string_lossy().is_empty() {
                    let dirs = dir
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().to_string());
                    self.meta
                        .write()
                        .expect("lock was poisoned")
                        .mkdir_all(Ino::Root, dirs)?
                        .ino
                } else {
                    Ino::Root
                };

                let len = entry
                    .metadata()
                    .map_err(|e| {
                        let kind = e
                            .io_error()
                            .map(|e| e.kind().into())
                            .unwrap_or(ErrorKind::Other);
                        Error::with_source(
                            kind,
                            format!(
                                "failed to access direntry metadata for {}",
                                entry.path().to_string_lossy()
                            ),
                            e,
                        )
                    })?
                    .len();
                self.meta.write().expect("lock was poisoned").create(
                    dir_ino,
                    name.to_string_lossy().to_string(),
                    true,
                    Location::staged(entry.path(), self.generation.load(Ordering::SeqCst)),
                    ByteRange { offset: 0, len },
                )?;
            }
        }

        self.commit(version).await
    }
}

macro_rules! try_sync {
    ($sync: expr, $join_err_cx: literal, $expr_err_cx: literal) => {
        match tokio::task::spawn_blocking(move || $sync).await {
            Ok(Ok(rv)) => Ok(rv),
            Ok(Err(e)) => Err(Error::with_source(e.kind().into(), $expr_err_cx, e)),
            Err(e) => Err(Error::with_source(
                ErrorKind::Other,
                concat!("spawn_blocking failed for ", $join_err_cx),
                e,
            )),
        }
    };
}

enum OpenMode {
    Read,
    ReadWrite,
    Create,
}

/// Opens a std::fs::File for reading and optionally writing.
async fn open_file(path: &Path, mode: OpenMode) -> Result<std::fs::File> {
    let copy = path.to_path_buf();

    let mut options = std::fs::File::options();
    match mode {
        OpenMode::Read => options.read(true),
        OpenMode::ReadWrite => options.read(true).write(true),
        OpenMode::Create => options.read(true).write(true).create(true).truncate(true),
    };

    try_sync!(
        options.open(copy),
        "std::fs::File::open",
        "failed to open staged file"
    )
}

async fn read_file_at(file: Arc<std::fs::File>, offset: u64, buf: &mut [u8]) -> Result<usize> {
    let size = buf.len();
    let bytes = try_sync!(
        {
            let mut bytes = bytes::BytesMut::zeroed(size);
            let n = file.read_at(&mut bytes, offset)?;
            bytes.truncate(n);
            Ok::<_, std::io::Error>(bytes.freeze())
        },
        "os::unix::fs::FileExt::read_at",
        "failed to read staged file"
    )?;

    let n = bytes.len();
    assert!(
        n <= size,
        "we should not have read more from the file than our buffer allows"
    );
    buf[..n].copy_from_slice(&bytes);
    Ok(n)
}

async fn write_file_at(file: Arc<std::fs::File>, offset: u64, buf: &[u8]) -> Result<usize> {
    let copy = buf.to_vec();
    try_sync!(
        file.write_at(&copy, offset),
        "os::unix::fs::FileExt::write_at",
        "failed to write staged file"
    )
}

fn copy_into(mut buf: &mut [u8], bytes: &[Bytes]) -> usize {
    let mut copied = 0;
    for bs in bytes {
        let n = std::cmp::min(buf.len(), bs.len());
        buf[..n].copy_from_slice(&bs[..n]);
        copied += n;
        buf = &mut buf[n..];
    }
    copied
}

// FIXME: this needs to allocate and check for remaining fds instead of just
// trying to increment every time and crashing. it's u64 so we probably won't
// hit it for a while but that's jank
fn new_fd(fd_set: &mut BTreeMap<Fd, FileDescriptor>, ino: Ino, d: FileDescriptor) -> Result<Fd> {
    let next_fh = fd_set
        .keys()
        .last()
        .cloned()
        .unwrap_or_default()
        .add_fh(1)?;
    let fd = Fd { ino, fh: next_fh };
    fd_set.insert(fd, d);
    Ok(fd)
}

macro_rules! try_mpu {
    ($mpu:expr, $context:literal) => {
        $mpu.await
            .map_err(|e| Error::with_source((&e).into(), $context, e))?
    };
}

struct Snapshot {
    metadata: VolumeMetadata,
    staged_files: Vec<(FileAttr, Arc<PathBuf>)>,
}

struct Commit<'a> {
    inner: &'a Volume,
}

impl<'a> Drop for Commit<'a> {
    fn drop(&mut self) {
        self.inner.commit.store(false, Ordering::SeqCst);
    }
}

impl<'a> Commit<'a> {
    fn new(inner: &'a Volume) -> Result<Self> {
        // only allow commits if another commit isn't already in-flight, set the commit bool if
        // we're okay to proceed.
        match inner
            .commit
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::new(
                    ErrorKind::ResourceBusy,
                    "a commit is already in progress",
                ));
            }
        }

        Ok(Self { inner })
    }

    /// The size of each part within the multipart upload (excluding the last which is allowed to
    /// be smaller). Aligned to 32MiB to help with alignment when reading from object storage.
    const MPU_UPLOAD_SIZE: usize = 32 * 1024 * 1024;
    const READ_BUF_SIZE: usize = 8 * 1024;

    /// Takes a snapshot of the metadata and the files we need to upload as a part of this commit
    /// while holding a lock for the metadata and file descriptor map.
    fn snapshot(&self) -> Result<Snapshot> {
        // read lock on metadata, others can read during the snapshot process, but can't modify.
        let metadata = self.inner.meta.read().expect("lock was poisoned");
        // write lock on the fds, no one is allowed to open new fds while we're taking a snapshot.
        let fds = self.inner.fds.write().expect("lock was poisoned");

        // don't allow open fds to staged files during the snapshot process.
        if fds
            .iter()
            .any(|(_, desc)| matches!(desc, FileDescriptor::Staged { .. }))
        {
            return Err(Error::new(
                ErrorKind::ResourceBusy,
                "all open staged files must be closed before committing",
            ));
        }

        // bump the generation number for new staged files. commit will only upload and take
        // snapshots of files with generation numbers less than this value. this draws a
        // line in the sand on what will be included in the commit. if someone opens a staged file
        // and sees that its generation number is less than the current generation, and commit is
        // inprogress, then we'll treat it similarly to opening a committed file (i.e. truncating
        // it).
        self.inner.generation.fetch_add(1, Ordering::SeqCst);

        let staged_files: Vec<_> = metadata
            .iter_staged()
            .map(|(attr, path)| (attr.clone(), path.clone()))
            .collect();

        Ok(Snapshot {
            metadata: metadata.clone(),
            staged_files,
        })
    }

    /// Upload all staged files into a single blob under base.
    ///
    /// Returns the location of the newly uploaded blob and a vector that maps each newly written
    /// Ino to its ByteRange within the new blob. Files are uploaded to the blob using multipart
    /// uploads.
    async fn upload_files(
        &self,
        files: Vec<(FileAttr, Arc<PathBuf>)>,
    ) -> Result<(Location, Vec<(Ino, ByteRange)>)> {
        let (dest_name, dest) = self.inner.store.new_data_file();

        let mut offset = 0;
        let mut staged = Vec::new();
        let mut writer = try_mpu!(
            self.inner.store.remote.put_multipart(&dest),
            "failed to start multipart upload"
        );

        let mut buf = BytesMut::with_capacity(Self::MPU_UPLOAD_SIZE);
        let mut read_buf = vec![0u8; Self::READ_BUF_SIZE];
        for (attr, path) in files {
            // don't actually upload anything for zero sized files
            if attr.size > 0 {
                let file = open_file(&path, OpenMode::Read).await?;
                let mut reader = BufReader::new(file).take(attr.size);

                loop {
                    // to avoid going over Self::MPU_UPLOAD_SIZE, limit how many bytes we read
                    // once we're close to the limit. this helps us push up perfectly aligned parts
                    // for the multipart upload.
                    let limit = Self::READ_BUF_SIZE.min(buf.capacity());
                    let n = reader.read(&mut read_buf[..limit]).map_err(|e| {
                        Error::with_source(e.kind().into(), "failed to read staged file", e)
                    })?;

                    if n == 0 {
                        break;
                    }

                    buf.extend_from_slice(&read_buf[..n]);

                    // we've hit our target upload size, so beam it up.
                    if buf.len() >= Self::MPU_UPLOAD_SIZE {
                        let frozen = buf.freeze();
                        try_mpu!(
                            writer.put_part(PutPayload::from_bytes(frozen.clone())),
                            "failed to upload part"
                        );
                        buf = match frozen.try_into_mut() {
                            // we're able to get the original buffer back, clear and reuse it
                            // to avoid reallocating
                            Ok(mut buf) => {
                                buf.clear();
                                buf
                            }
                            // we can't get the original buffer back which a reference is still
                            // alive out there.
                            Err(_) => BytesMut::with_capacity(Self::MPU_UPLOAD_SIZE),
                        };
                    }
                }
            }

            staged.push((attr.ino, (offset, attr.size).into()));
            offset += attr.size;
        }

        // last part within an multi-part upload can be less than 5MiB
        if !buf.is_empty() {
            try_mpu!(
                writer.put_part(PutPayload::from_bytes(buf.into())),
                "failed to upload part"
            );
        }

        try_mpu!(writer.complete(), "failed to complete upload");

        Ok((Location::committed(dest_name), staged))
    }

    /// Mint and upload a new version of Volume.
    async fn upload_metadata(&self, metadata: &mut VolumeMetadata, version: Version) -> Result<()> {
        let meta_path = self.inner.store.metadata_path(&version);
        let new_volume = bytes::Bytes::from(metadata.to_bytes_with_version(&version)?);

        let put_metadata = || async {
            self.inner
                .store
                .remote
                .put_opts(
                    &meta_path,
                    // bytes::Bytes is Arc'd internally, no real copying here
                    PutPayload::from_bytes(new_volume.clone()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        };
        let res = put_metadata
            .retry(ExponentialBuilder::default())
            .when(should_retry)
            .await;

        match res {
            Ok(_) => {
                metadata.set_version(version);
                Ok(())
            }
            // TODO: there's a scenario here where we get an AlreadyExists error returned to us,
            // but we did write a new metadata version. this can happen if our first attempt at the
            // metadata PUT returned an error after it successfully uploaded it, and we do a
            // retry which tells us it already exists. in this scenario, the most correct thing
            // would be to read the existing file and see if it's identical to the current metadata
            // we have. if it is, then this can return Ok(()) and we can bump the version. if not,
            // then it was a race condition where some other mount actually wrote to the same version
            // before we could.
            Err(object_store::Error::AlreadyExists { source, .. }) => Err(Error::with_source(
                ErrorKind::AlreadyExists,
                "version already exists",
                source,
            )),
            Err(e) => Err(Error::with_source(
                (&e).into(),
                "failed to upload volume metadata",
                e,
            )),
        }
    }

    fn sync(self, snapshot: VolumeMetadata) -> Result<()> {
        let mut metadata = self.inner.meta.write().expect("lock was poisoned");
        metadata.set_version(snapshot.version().clone());

        let snapshot_generation = self.inner.generation.load(Ordering::SeqCst);
        // walk the snapshot, updating the current view of metadata for the dir entries for staged
        // regular files that we committed up to S3. only update them if they weren't updated since
        // we took the snapshot. if they were updated, then any concurrent updates to the metdata
        // while we were committing will take precedence.
        for snapshot_entry in snapshot.walk(Ino::Root)? {
            let snapshot_entry = snapshot_entry?;
            if !snapshot_entry.is_regular() || !snapshot_entry.attr.is_file() {
                continue;
            }

            // fetch the equivalent entry in the current version of metadata. if it's missing, then
            // someone deleted this ino while we were busy committing.
            let ino = snapshot_entry.attr.ino;
            let Some(attr) = metadata.getattr(ino).cloned() else {
                continue;
            };

            // if the location is still staged and has an older generation, then we know that it
            // wasn't opened for writing since the commit. we can proceed with replacing its
            // location to the one we uploaded to S3.
            if !matches!(
                metadata.location(ino),
                Some((Location::Staged { generation, .. }, _)) if generation < snapshot_generation
            ) {
                continue;
            }

            // retain mtime and ctime changes (e.g. if the file was touched to update mtime or
            // moved)
            let new_mtime = std::cmp::max(attr.mtime, snapshot_entry.attr.mtime);
            let new_ctime = std::cmp::max(attr.ctime, snapshot_entry.attr.ctime);

            let Some((snapshot_location, snapshot_byte_range)) = snapshot_entry.location() else {
                continue;
            };

            metadata.modify(
                ino,
                new_mtime,
                Some(new_ctime),
                Some(snapshot_location.clone()),
                Some(Modify::Set(snapshot_byte_range)),
            )?;
        }

        // the walk and location modification will result in some unreferenced staged locations.
        // these need to be cleaned up.
        metadata.prune_unreferenced_locations();

        Ok(())
    }
}

/// Updates the entries within `metadata` specified by the Inos in `ranges` to point to `dest` and
/// have the range in `ranges`.
fn apply_location_ranges(
    metadata: &mut VolumeMetadata,
    ranges: Vec<(Ino, ByteRange)>,
    dest: Location,
) -> Result<()> {
    let now = SystemTime::now();
    for (ino, byte_range) in ranges {
        metadata.modify(
            ino,
            now,
            Some(now),
            Some(dest.clone()),
            Some(Modify::Set(byte_range)),
        )?;
    }

    metadata.prune_unreferenced_locations();

    Ok(())
}

fn should_retry(e: &object_store::Error) -> bool {
    use std::error::Error;

    let mut source = e.source();
    while let Some(e) = source {
        if let Some(http_error) = e.downcast_ref::<object_store::client::HttpError>() {
            return matches!(
                http_error.kind(),
                // these are the only two that are not retried with PutMode::Create since they
                // don't consider it idempotent. we'll retry these ourselves.
                object_store::client::HttpErrorKind::Timeout
                    | object_store::client::HttpErrorKind::Interrupted
            );
        }
        source = e.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Client;

    #[test]
    fn test_copy_into() {
        // buf.len() = sum(bytes.len())
        {
            let mut buf = [0u8; 10];
            let bytes = &[
                Bytes::from_static(b"123"),
                Bytes::from_static(b"456"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"890"),
            ];
            assert_eq!(10, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..10], b"1234567890");
        }
        // buf.len() > sum(bytes.len())
        {
            let mut buf = [0u8; 10];
            let bytes = &[Bytes::from_static(b"123"), Bytes::from_static(b"456")];
            assert_eq!(6, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..6], b"123456");
        }
        // buf.len() < sum(bytes.len())
        {
            let mut buf = [0u8; 5];
            let bytes = &[
                Bytes::from_static(b"123"),
                Bytes::from_static(b"456"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"890"),
            ];
            assert_eq!(5, copy_into(&mut buf, bytes));
            assert_eq!(&buf[0..5], b"12345");
        }
    }

    async fn assert_write(volume: &Volume, fd: Fd, contents: &'static str) {
        volume.write_at(fd, 0, contents.as_bytes()).await.unwrap();
        volume.release(fd).await.unwrap();
    }

    async fn assert_read(volume: &Volume, name: &'static str, contents: &'static str) {
        let attr = volume.lookup(Ino::Root, name).unwrap().unwrap();
        let mut buf = vec![0u8; attr.size as usize];
        let fd = volume.open_read(attr.ino).await.unwrap();
        let n = volume.read_at(fd, 0, &mut buf).await.unwrap();
        assert_eq!(n, contents.len());
        assert_eq!(buf, contents.as_bytes());
    }

    #[test]
    fn read_at() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let volume_path = tempdir.path().join("store");
        std::fs::create_dir_all(&volume_path).unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        // create a volume with three files, all of which are 5 bytes long.
        let (volume, inos) = runtime.block_on(async {
            let mut client = Client::open(volume_path.to_str().unwrap()).unwrap();
            let volume = client.create_volume().await;

            let create = async |name, bs| {
                let (attr, fd) = volume
                    .create(Ino::Root, std::primitive::str::to_string(name), true)
                    .await
                    .unwrap();
                let ino = attr.ino;
                volume.write_at(fd, 0, bs).await.unwrap();
                volume.release(fd).await.unwrap();
                ino
            };

            let a = create("a", b"aaaaa").await;
            let b = create("b", b"bbbbb").await;
            let c = create("c", b"ccccc").await;
            volume.commit(Version::from_static("v1")).await.unwrap();

            (volume, [a, b, c])
        });

        // generate a random set of reads from all three files. all of the reads
        // are at most 16 bytes, and will have a random offset in 0..10. this should
        // generate a decent mix of in-bounds nad out-of-bounds reads
        arbtest::arbtest(|u| {
            let ino = u.choose(&inos)?;
            let offset = u.int_in_range(0..=10)?;
            let mut buf = [0u8; 16];

            runtime.block_on(async {
                let fd = volume.open_read(*ino).await.unwrap();
                volume.read_at(fd, offset, &mut buf).await.unwrap();
                volume.release(fd).await.unwrap();
            });

            Ok(())
        });
    }

    #[tokio::test]
    async fn commit() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let volume_path = tempdir.path().join("store");
        std::fs::create_dir_all(&volume_path).unwrap();

        let mut client = Client::open(volume_path.to_str().unwrap()).unwrap();
        let volume = client.create_volume().await;

        // clean volume -- this is not staged
        assert!(!volume.meta.read().expect("lock was poisoned").is_staged());

        // creating two files, it should be a staged volume now.
        let (attr1, fd1) = volume
            .create(Ino::Root, "hello.txt".into(), true)
            .await
            .unwrap();
        let attr1 = attr1.clone();
        assert_write(&volume, fd1, "hello").await;
        let (attr2, fd2) = volume
            .create(Ino::Root, "world.txt".into(), true)
            .await
            .unwrap();
        let attr2 = attr2.clone();
        assert_write(&volume, fd2, "world").await;

        {
            let meta = volume.meta.read().expect("lock was poisoned");
            assert!(meta.is_staged());
            for attr in [&attr1, &attr2] {
                assert!(matches!(
                    meta.location(attr.ino),
                    Some((Location::Staged { .. }, _))
                ));
            }
        }

        // commit!!!
        let commit_fd = volume.open_read_write(Ino::COMMIT).await.unwrap();
        assert_write(&volume, commit_fd, "next-version").await;

        // after commit, both files are no longer staged
        {
            let meta = volume.meta.read().expect("lock was poisoned");
            assert!(!meta.is_staged());
            for attr in [&attr1, &attr2] {
                assert!(matches!(
                    meta.location(attr.ino),
                    Some((Location::Committed { .. }, _))
                ));
            }
        }

        // read the new volume, assert that the committed files have the
        // right contents, and check the version is bumped as expected
        let next_volume = client.load_volume(&None).await.unwrap();
        let next_version = next_volume.version().to_string();
        assert_eq!(next_version, "next-version");
        assert_read(&next_volume, "hello.txt", "hello").await;
        assert_read(&next_volume, "world.txt", "world").await;
    }

    #[tokio::test]
    async fn commit_with_open_fds() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let volume_path = tempdir.path().join("store");
        std::fs::create_dir_all(&volume_path).unwrap();

        let mut client = Client::open(volume_path.to_str().unwrap()).unwrap();
        let volume = client.create_volume().await;

        // creating a file, but holding the fd (not releasing it)
        let (attr, fd) = volume
            .create(Ino::Root, "hello.txt".into(), true)
            .await
            .unwrap();
        let attr = attr.clone();
        assert!(matches!(
            volume
                .meta
                .read()
                .expect("lock was poisoned")
                .location(attr.ino),
            Some((Location::Staged { .. }, _))
        ));

        // commit fails because we have a staged fd open
        assert_eq!(
            volume
                .commit(Version::from_static("next-version"))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::ResourceBusy
        );

        // close it and we're okay to commit
        volume.release(fd).await.unwrap();
        volume
            .commit(Version::from_static("next-next-version"))
            .await
            .unwrap();

        // open the file we just committed, along with a bunch of random special files
        let _f0 = volume.open_read(Ino::VERSION).await.unwrap();
        let _f1 = volume.open_read(Ino::PROM_METRICS).await.unwrap();
        let _f2 = volume.open_read_write(Ino::CLEAR_CACHE).await.unwrap();
        let _f3 = volume.open_read_write(Ino::COMMIT).await.unwrap();
        let _f4 = volume.open_read(attr.ino).await.unwrap();
        // this is ok, because none of them are staged
        assert!(!volume.fds.read().expect("lock was poisoned").is_empty());
        volume
            .commit(Version::from_static("next-version"))
            .await
            .unwrap();
    }
}
