use crate::{
    ByteRange, DirEntry, Error, FileAttr, Ino, Location, Result,
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
    path::Path,
    sync::Arc,
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

#[derive(Debug)]
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

pub struct Volume {
    meta: VolumeMetadata,
    cache: ChunkCache,
    fds: BTreeMap<Fd, FileDescriptor>,
    store: crate::storage::Storage,
    metrics_snapshot: Option<Arc<ArcSwap<Vec<u8>>>>,
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
            meta,
            cache,
            fds: Default::default(),
            store,
            metrics_snapshot,
        }
    }

    pub(crate) fn metadata(&self) -> &VolumeMetadata {
        &self.meta
    }

    pub(crate) fn metadata_mut(&mut self) -> &mut VolumeMetadata {
        &mut self.meta
    }

    pub(crate) fn modify(
        &mut self,
        ino: Ino,
        mtime: SystemTime,
        ctime: Option<SystemTime>,
        location: Option<Location>,
        range: Option<Modify>,
    ) -> Result<()> {
        match ino {
            Ino::CLEAR_CACHE | Ino::COMMIT => Ok(()),
            ino => {
                self.meta.modify(ino, mtime, ctime, location, range)?;
                Ok(())
            }
        }
    }

    pub fn version(&self) -> &Version {
        self.metadata().version()
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
        self.meta.to_bytes()
    }

    pub fn to_bytes_with_version(&self, version: &Version) -> Result<Vec<u8>> {
        self.meta.to_bytes_with_version(version)
    }

    pub fn getattr(&self, ino: Ino) -> Result<&FileAttr> {
        scoped_timer!("pond_volume_getattr_latency_secs");
        match self.meta.getattr(ino) {
            Some(attr) => Ok(attr),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub fn setattr(
        &mut self,
        ino: Ino,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
    ) -> Result<&FileAttr> {
        self.meta.setattr(ino, mtime, ctime)
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Result<Option<&FileAttr>> {
        scoped_timer!("pond_volume_lookup_latency_secs");
        let attr = self.meta.lookup(parent, name)?;
        Ok(attr)
    }

    pub fn mkdir(&mut self, parent: Ino, name: String) -> Result<&FileAttr> {
        self.meta.mkdir(parent, name)
    }

    pub fn rmdir(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.meta.rmdir(parent, name)?;
        Ok(())
    }

    pub fn rename(
        &mut self,
        parent: Ino,
        name: &str,
        newparent: Ino,
        newname: String,
    ) -> Result<()> {
        self.meta.rename(parent, name, newparent, newname)?;
        Ok(())
    }

    pub fn readdir(&self, ino: Ino) -> Result<impl Iterator<Item = DirEntry<'_>>> {
        let iter = self.meta.readdir(ino)?;
        Ok(iter)
    }

    pub fn create(
        &mut self,
        parent: Ino,
        name: String,
        exclusive: bool,
    ) -> Result<(&FileAttr, Fd)> {
        let (path, file) = self.store.tempfile()?;

        let attr = self.meta.create(
            parent,
            name,
            exclusive,
            Location::Staged { path },
            ByteRange::empty(),
        )?;

        let fd = new_fd(
            &mut self.fds,
            attr.ino,
            FileDescriptor::Staged { file: file.into() },
        )?;
        Ok((attr, fd))
    }

    pub fn delete(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.meta.delete(parent, name)?;
        Ok(())
    }

    pub fn truncate(&mut self, ino: Ino, size: u64) -> Result<()> {
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
    pub async fn open_read_write(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::COMMIT => new_fd(&mut self.fds, ino, FileDescriptor::Commit),
            Ino::CLEAR_CACHE => new_fd(&mut self.fds, ino, FileDescriptor::ClearCache),
            Ino::VERSION => Err(ErrorKind::PermissionDenied.into()),
            ino => match self.meta.location(ino) {
                Some((Location::Staged { path }, _)) => {
                    let file = open_file(path, true).await?;

                    new_fd(
                        &mut self.fds,
                        ino,
                        FileDescriptor::Staged { file: file.into() },
                    )
                }
                Some((Location::Committed { .. }, ..)) => {
                    // truncate the file (by assigning it a brand new staged file) if it's
                    // committed. the alternative would be to keep a copy of the committed
                    // file locally as a staged file, which can be expensive if it's a large file.
                    let (path, file) = self.store.tempfile()?;
                    let staged = Location::Staged { path };
                    // modify metadata next
                    self.modify(
                        ino,
                        SystemTime::now(),
                        None,
                        Some(staged),
                        Some(Modify::Set((0, 0).into())),
                    )?;
                    // only create the fd once the file is open and metadata is valid
                    new_fd(
                        &mut self.fds,
                        ino,
                        FileDescriptor::Staged { file: file.into() },
                    )
                }
                None => Err(ErrorKind::NotFound.into()),
            },
        }
    }

    pub async fn open_read(&mut self, ino: Ino) -> Result<Fd> {
        match ino {
            Ino::VERSION => new_fd(&mut self.fds, ino, FileDescriptor::Version),
            Ino::PROM_METRICS => {
                let data = match &self.metrics_snapshot {
                    Some(metrics) => metrics.load().clone(),
                    None => Default::default(),
                };
                new_fd(
                    &mut self.fds,
                    ino,
                    FileDescriptor::PromMetrics { snapshot: data },
                )
            }
            Ino::COMMIT | Ino::CLEAR_CACHE => Err(ErrorKind::PermissionDenied.into()),
            ino => match self.meta.location(ino) {
                Some((Location::Staged { path }, _)) => {
                    let file = open_file(path, false).await?;
                    new_fd(
                        &mut self.fds,
                        ino,
                        FileDescriptor::Staged { file: file.into() },
                    )
                }
                Some((Location::Committed { key }, range)) => {
                    let key = Arc::new(self.store.child_path(key));
                    new_fd(
                        &mut self.fds,
                        ino,
                        FileDescriptor::Committed { key, range: *range },
                    )
                }
                None => Err(ErrorKind::NotFound.into()),
            },
        }
    }

    pub async fn read_at(&self, fd: Fd, offset: u64, buf: &mut [u8]) -> Result<usize> {
        metrics::histogram!("pond_volume_read_buf_size_bytes").record(buf.len() as f64);

        match self.fds.get(&fd) {
            // reads of write-only special fds do nothing
            Some(FileDescriptor::ClearCache) | Some(FileDescriptor::Commit) => Ok(0),
            Some(FileDescriptor::Version) => read_version(self.meta.version(), offset, buf),
            Some(FileDescriptor::PromMetrics { snapshot }) => read_from_buf(snapshot, offset, buf),
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
                let bytes: Vec<Bytes> = self
                    .cache
                    .get_at(key.clone(), blob_offset, read_len)
                    .await?;
                Ok(copy_into(buf, &bytes))
            }
            Some(FileDescriptor::Staged { file, .. }) => {
                scoped_timer!("pond_volume_read_latency_secs", "type" => "staged");
                read_file_at(file.clone(), offset, buf).await
            }
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn write_at(&mut self, fd: Fd, offset: u64, data: &[u8]) -> Result<usize> {
        metrics::histogram!("pond_volume_write_buf_size_bytes").record(data.len() as f64);

        match self.fds.get_mut(&fd) {
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

    pub async fn release(&mut self, fd: Fd) -> Result<()> {
        match self.fds.remove(&fd) {
            Some(_) => Ok(()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    pub async fn commit(&mut self, version: Version) -> Result<()> {
        if self.store.exists(&version).await? {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("version {} already exists", &version),
            ));
        }

        if !self.fds.is_empty() {
            let num_open_fds = self.fds.len();
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "all open files must be closed before committing. files open: {num_open_fds}"
                ),
            ));
        }

        let mut staged = StagedVolume::new(self);
        let (dest, ranges) = staged.upload().await?;
        staged.modify(dest, ranges)?;
        staged.persist(version).await?;

        Ok(())
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

impl Volume {
    pub fn walk(&self, ino: Ino) -> Result<impl Iterator<Item = Result<DirEntry<'_>>>> {
        self.meta.walk(ino)
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
                self.metadata_mut().mkdir_all(Ino::Root, dirs)?;
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
                    self.metadata_mut().mkdir_all(Ino::Root, dirs)?.ino
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
                self.metadata_mut().create(
                    dir_ino,
                    name.to_string_lossy().to_string(),
                    true,
                    Location::staged(entry.path()),
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

/// Opens a std::fs::File for reading (and writing if `write` is set).
async fn open_file(path: &Path, write: bool) -> Result<std::fs::File> {
    let copy = path.to_path_buf();
    try_sync!(
        std::fs::File::options().read(true).write(write).open(copy),
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

struct StagedVolume<'a> {
    inner: &'a mut Volume,
}

impl<'a> StagedVolume<'a> {
    fn new(inner: &'a mut Volume) -> Self {
        Self { inner }
    }

    /// The size of each part within the multipart upload (excluding the last which is allowed to
    /// be smaller). Aligned to 32MiB to help with alignment when reading from object storage.
    const MPU_UPLOAD_SIZE: usize = 32 * 1024 * 1024;
    const READ_BUF_SIZE: usize = 8 * 1024;

    /// Upload all staged files into a single blob under base.
    ///
    /// Returns the location of the newly uploaded blob and a vector that maps each newly written
    /// Ino to its ByteRange within the new blob. Files are uploaded to the blob using multipart
    /// uploads.
    async fn upload(&self) -> Result<(Location, Vec<(Ino, ByteRange)>)> {
        let (dest_name, dest) = self.inner.store.new_data_file();

        let mut offset = 0;
        let mut staged = Vec::new();
        let mut writer = try_mpu!(
            self.inner.store.remote.put_multipart(&dest),
            "failed to start multipart upload"
        );

        let mut buf = BytesMut::with_capacity(Self::MPU_UPLOAD_SIZE);
        let mut read_buf = vec![0u8; Self::READ_BUF_SIZE];
        for (attr, path) in self.inner.meta.iter_staged() {
            // don't actually upload anything for zero sized files
            if attr.size > 0 {
                let file = open_file(path, false).await?;
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

    /// Relocate all staged files to dest.
    fn modify(&mut self, dest: Location, ranges: Vec<(Ino, ByteRange)>) -> Result<()> {
        let now = SystemTime::now();
        for (ino, byte_range) in ranges {
            self.inner.meta.modify(
                ino,
                now,
                Some(now),
                Some(dest.clone()),
                Some(Modify::Set(byte_range)),
            )?;
        }

        // deduplicate and clean up all hanging staged Locations
        self.inner.meta.clean_staged_locations(dest);

        Ok(())
    }

    /// Mint and upload a new version of Volume.
    async fn persist(self, version: Version) -> Result<()> {
        let meta_path = self.inner.store.metadata_path(&version);
        let new_volume = bytes::Bytes::from(self.inner.to_bytes_with_version(&version)?);

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
                self.inner.meta.set_version(version);
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

    async fn assert_write(volume: &mut Volume, fd: Fd, contents: &'static str) {
        volume.write_at(fd, 0, contents.as_bytes()).await.unwrap();
        volume.release(fd).await.unwrap();
    }

    async fn assert_read(volume: &mut Volume, name: &'static str, contents: &'static str) {
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
        let (mut volume, inos) = runtime.block_on(async {
            let mut client = Client::open(volume_path.to_str().unwrap()).unwrap();
            let mut volume = client.create_volume().await;

            let mut create = async |name, bs| {
                let (attr, fd) = volume
                    .create(Ino::Root, std::primitive::str::to_string(name), true)
                    .unwrap();
                let ino = attr.ino;
                volume.write_at(fd, 0, bs).await.unwrap();
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
        let mut volume = client.create_volume().await;

        // clean volume -- this is not staged
        assert!(!volume.meta.is_staged());

        // creating two files, it should be a staged volume now.
        let (attr1, fd1) = volume.create(Ino::Root, "hello.txt".into(), true).unwrap();
        let attr1 = attr1.clone();
        assert_write(&mut volume, fd1, "hello").await;
        let (attr2, fd2) = volume.create(Ino::Root, "world.txt".into(), true).unwrap();
        let attr2 = attr2.clone();
        assert_write(&mut volume, fd2, "world").await;

        assert!(volume.meta.is_staged());
        for attr in [&attr1, &attr2] {
            assert!(matches!(
                volume.meta.location(attr.ino),
                Some((Location::Staged { .. }, _))
            ));
        }

        // commit!!!
        let commit_fd = volume.open_read_write(Ino::COMMIT).await.unwrap();
        assert_write(&mut volume, commit_fd, "next-version").await;

        // after commit, both files are no longer staged
        assert!(!volume.meta.is_staged());
        for attr in [&attr1, &attr2] {
            assert!(matches!(
                volume.meta.location(attr.ino),
                Some((Location::Committed { .. }, _))
            ));
        }

        // read the new volume, assert that the committed files have the
        // right contents, and check the version is bumped as expected
        let mut next_volume = client.load_volume(&None).await.unwrap();
        let next_version: &str = next_volume.meta.version().as_ref();
        assert_eq!(next_version, "next-version",);
        assert_read(&mut next_volume, "hello.txt", "hello").await;
        assert_read(&mut next_volume, "world.txt", "world").await;
    }
}
